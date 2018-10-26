using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public unsafe sealed class StaticFixedSizeHashTable<TKey, TValue>: IDisposable, IDictionary<TKey, TValue> where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><record-padding>

        private short maxAllowedDistance;
        const int keyOffset = 0;
        int valueOffset;
        int distanceOffset;
        readonly bool isAligned;

        int keySize;
        int valueSize;
        int distanceSize;
        internal int recordSize;
        internal readonly long slotCount;
        readonly long hashMask;

        private MemoryMapper memoryMapper;
        private MemoryMappingSession mappingSession;
        private byte* fileBaseAddress;

        private StaticFixedSizeHashTableFileHeader* headerPointer;
        internal readonly byte* tablePointer;
        internal readonly byte* endTablePointer;

        public readonly ThreadSafety ThreadSafety;
        private int syncObjectCount;
#if SPINLATCH
        internal int[] syncObjects;
#else
        internal SyncObject[] syncObjects;
#endif
        private int maxLocksPerOperation;
        private long chunkSize;
        internal int chunkBits;
        private long chunkMask;

        

        /// <summary>
        /// Max distance ever seen in the hash table. 
        /// MaxDistance is updated only on adding, It is not uptaded on removing.
        /// </summary>
        // MaxDistance starts with 0 while internal distance starts with 1. So, it is the real max distance.
        public int MaxDistance => headerPointer->MaxDistance;

        //Note that float casts are not redundant as VS says.
        public float LoadFactor => (float)Count / (float)slotCount;
        public float MeanDistance => (float)headerPointer->DistanceSum / (float)Count;

        private readonly Func<TKey, long> hashFunction;
        private readonly IEqualityComparer<TKey> comparer;
        private readonly IEqualityComparer<TValue> valueComparer;
        private readonly long Capacity;


        public long Count
        {
            get { return headerPointer->RecordCount; }
            private set { headerPointer->RecordCount = value; }
        }



        public StaticFixedSizeHashTable(string filePath, long capacity, ThreadSafety threadSafety, Func<TKey, long> hashFunction = null, IEqualityComparer<TKey> comparer = null,  bool isAligned = false)
        {
            this.isAligned = isAligned;
            this.hashFunction = hashFunction;
            this.comparer = comparer ?? EqualityComparer<TKey>.Default;
            this.valueComparer = EqualityComparer<TValue>.Default;
            CalculateOffsetsAndSizesDependingOnAlignement();
            slotCount = Math.Max(capacity + capacity/8 + capacity/16, 4);
            slotCount = Bits.IsPowerOfTwo(slotCount) ? slotCount : Bits.NextPowerOf2(slotCount);
            this.Capacity = slotCount - slotCount/8 - capacity/16; // conservative max load factor = 81.25%
            hashMask = slotCount - 1L;
            this.ThreadSafety = threadSafety;
            var fileSize = (long) sizeof(StaticFixedSizeHashTableFileHeader) +  slotCount * recordSize;
            fileSize += (Constants.AllocationGranularity - (fileSize & (Constants.AllocationGranularity - 1))) & (Constants.AllocationGranularity - 1);

            var isNew = !File.Exists(filePath);

            memoryMapper = new MemoryMapper(filePath, (long) fileSize);
            mappingSession = memoryMapper.OpenSession();

            fileBaseAddress = mappingSession.GetBaseAddress();
            headerPointer = (StaticFixedSizeHashTableFileHeader*)fileBaseAddress;


            if (isNew)
            {
                InitializeHeader();
            }
            else
            {
                ValidateHeader();
                slotCount = headerPointer->SlotCount;
            }
            tablePointer = fileBaseAddress + sizeof(StaticFixedSizeHashTableFileHeader);
            endTablePointer = tablePointer + recordSize * slotCount;
            maxAllowedDistance = (short) Math.Max(slotCount, 448);

            InitializeSynchronization();
        }

        private void InitializeSynchronization()
        {
            /*
             * We use System.Threading.Monitor To achieve synchronization
             * The table is divided into equal sized chunks of slots.
             * We use an array of sync objects with one sync object per chunk.
             * The sync object associated with a chunk is locked when accesing slots in the chunk.
             * If the record distance is greater than chunk size, more than one sync object will be locked.
             */

            if (this.ThreadSafety == ThreadSafety.Unsafe) return;
            /*
             According to the Birthday problem and using the Square approximation
             p(n) = n^2/2/m. where p is the probalitity, n is the number of people and m is the number of days in a year.
             m = n^2/2/p(n)
             syncObjectCount maps to the number of days in a year (m), 
             numer of threads accessing hash table maps to number of people (n).
             we are guessing number of threads = Environment.ProcessorCount * 2  
             and we want a probability of 1/8 that a thread gets blocked.

             we are constraining syncObjectCount to be between 128 and 8192
            */
            syncObjectCount = Math.Min(Math.Max( Environment.ProcessorCount * Environment.ProcessorCount * 16, 128), 8192);

            // but we need a power of two
            if (Bits.IsPowerOfTwo(syncObjectCount) == false) syncObjectCount = Math.Min(Bits.NextPowerOf2(syncObjectCount), 8192);


            // max locks per operation cannot be greater than 8, 
            // we are using one long as an array of 8 bools to keep track of locked sync objects
            // we want at least 64 slots per chunk. 
            // with 8 locks and 64 slots per chunk, we can cover 8*64 = 512 slots and (8 - 1)* 64 = 448 maxAllowedDistance  
            // Most of the time, when distance < 64, we only need to lock one sync object
            // Chunk size cannot be greater than the number of slots
            chunkSize = Math.Min(Math.Max(slotCount / syncObjectCount, 64), slotCount);
            chunkMask = chunkSize - 1;
            chunkBits = Bits.MostSignificantBit(chunkSize);

            // As said, max locks per operation cannot be greater than 8. 
            // We must satisfy (MaxLocksPerOperation - 1) * ChunkSize >= MaxAllowedDistance,
            // We don't want to allow a a distance greater than 448. 
            // chunkSize == 1 is an edge case.
            maxAllowedDistance = chunkSize == 1 ? (short)8: (short) Math.Min(7 * chunkSize, 448);

            // We use an array of max locks per operation booleans to keep track of locked sync objects
            maxLocksPerOperation = chunkSize == 1 ?  8 : Math.Max((int)(maxAllowedDistance / chunkSize + 1L), 2);

            // recalc with constraints applied 
            syncObjectCount = (int)(slotCount / chunkSize);

            Debug.Assert(Bits.IsPowerOfTwo(chunkSize));
            Debug.Assert((maxLocksPerOperation > 1 || chunkSize == 1 && maxLocksPerOperation == 1) && maxLocksPerOperation <= 8);
            Debug.Assert(((maxLocksPerOperation - 1) * chunkSize >= maxAllowedDistance) || chunkSize == 1);


           
#if SPINLATCH
            syncObjects = new int[syncObjectCount];
            for (int i = 0; i < syncObjectCount; i++) syncObjects[i] = 0;
#else
            syncObjects = new SyncObject[syncObjectCount];
            for (int i = 0; i < syncObjectCount; i++) syncObjects[i] = new SyncObject();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(long slotIndex) => 
            tablePointer + recordSize * slotIndex;

        /// <summary>
        /// Returns the distance + 1 to the initial slot index.
        /// The initial slot index is the slot index where the hash maps to.
        /// distance == 0 means the slot is empty. distance > 0 means the slot is occupied
        /// The distance is incremented by 1 to reserve zero value as empty slot.
        /// </summary>
        /// <param name="recordPointer"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal short GetDistance(byte* recordPointer) => 
            *(short*)(recordPointer + distanceOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetDistance(byte* recordPointer, short distance)
        {
            *(short*)(recordPointer + distanceOffset) = distance;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetKey(byte* recordPointer, TKey key)
        {
            *(TKey*)(recordPointer + keyOffset) = key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetValue(byte* recordPointer, TValue value)
        {
            *(TValue*)(recordPointer + valueOffset) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetKeyPointer(byte* recordPointer) => 
            recordPointer + keyOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetKeyAsSpan(byte* keyPointer) => 
            new ReadOnlySpan<byte>(keyPointer, (int)keySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TKey GetKey(byte* keyPointer) => 
            *(TKey*)keyPointer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetValuePointer(byte* recordPointer) => 
            recordPointer + valueOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetValueAsSpan(byte *valuePointer) => 
            new ReadOnlySpan<byte>(valuePointer, (int)valueSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static internal TValue GetValue(byte* valuePointer) => 
            *(TValue*)valuePointer;

        void InitializeHeader()
        {
            headerPointer->IsAligned = isAligned;
            headerPointer->DistanceSum = 0;
            headerPointer->KeySize = keySize;
            headerPointer->Magic = StaticFixedSizeHashTableFileHeader.MagicNumber;
            headerPointer->MaxDistance = 0;
            headerPointer->RecordCount = 0;
            headerPointer->RecordSize = recordSize;
            headerPointer->Reserved[0] = 0;
            headerPointer->SlotCount = slotCount;
            headerPointer->ValueSize = valueSize;
        }

        void ValidateHeader()
        {
            if ( headerPointer->Magic != StaticFixedSizeHashTableFileHeader.MagicNumber)
            {
                throw new FormatException($"This is not a {nameof(StaticFixedSizeHashTable<TKey, TValue>)} file");
            }
            if (headerPointer->IsAligned != isAligned)
            {
                throw new ArgumentException("Mismatched alignement");
            }
            if (headerPointer->KeySize != keySize)
            {
                throw new ArgumentException("Mismatched keySize");
            }
            if (headerPointer->ValueSize != valueSize)
            {
                throw new ArgumentException("Mismatched ValueSize");
            }
            if (headerPointer->RecordSize != recordSize)
            {
                throw new ArgumentException("Mismatched SlotSize");
            }
        }

        private void CalculateOffsetsAndSizesDependingOnAlignement()
        {
            keySize = sizeof(TKey);
            valueSize = sizeof(TValue);
            distanceSize = sizeof(short);


            int keyAlignement = GetAlignement(keySize);
            int valueAlignement = GetAlignement(valueSize);
            int distanceAlignement = GetAlignement(distanceSize);
            int slotAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            //keyOffset = 0;
            valueOffset = keyOffset + keySize + GetPadding(keyOffset + keySize, valueAlignement);
            distanceOffset = valueOffset + valueSize + GetPadding(valueOffset + valueSize, distanceAlignement);
            recordSize = distanceOffset + distanceSize + GetPadding(distanceOffset + distanceSize, slotAlignement);
        }

        private int GetPadding(int offsetWithoutPadding, int alignement)
        {
            if (!isAligned) return 0;
            int alignementMinusOne = alignement - 1;
            // (alignement - offsetWithoutPadding % alignement) % alignement
            return (alignement - (offsetWithoutPadding & alignementMinusOne)) & alignementMinusOne;
        }

        private int GetAlignement(int size)
        {
            if (!isAligned) return 1;
            if (size >= 8) return 8;
            if (size >= 4) return 4;
            if (size >= 2) return 2;
            return 1;
        }

        /// <summary>
        /// Returns the intial slot index corresponding to the key. 
        /// It is the index that the key hash maps to.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetInitialSlot(TKey key)
        {
            if (hashFunction == null)
            {
                return Hashing.FastHash64((byte*)&key, keySize) & hashMask;
            }
            else
            {
                //return hashFunction(key) * 11400714819323198485LU & mask;
                return hashFunction(key) & hashMask;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void InitializeOperationContext(ref OperationContext context, TKey key, void* takenLocks)
        {
            context.CurrentSlot = context.InitialSlot = GetInitialSlot(key);
            context.Key = key;
            context.TakenLocks = (bool*)takenLocks;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    value = default;
                    return false;
                }
                value = GetValue(GetValuePointer(recordPointer));
                return true;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool TryGetValueNonBlocking(TKey key, out TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    value = default;
                    return false;
                }
                value = GetValue(GetValuePointer(recordPointer));
                return true;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public void Add(TKey key, TValue value)
        {
            if (!TryAdd(key, value))
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    RobinHoodAdd(ref context, value);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool ContainsKey(TKey key)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                return FindRecord(ref context) != null;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public void Put(TKey key, TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    RobinHoodAdd(ref context, value);
                }
                else
                {
                    SetValue(recordPointer, value);
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RobinHoodAdd(ref OperationContext context,  TValue value)
        {
            byte* recordPointer =  GetRecordPointer(context.InitialSlot);
            short distance = 1; //start with 1 because 0 is reserved for empty slots.

            // set context to initial slot
            context.LockIndex = 0;
            context.RemainingSlotsInChunk = 0;
            context.CurrentSlot = context.InitialSlot;

            TKey key = context.Key;
            while(true)
            {
                LockIfNeeded(ref context);
                short currentRecordDistance = GetDistance(recordPointer);
                if (currentRecordDistance == 0) // found empty slot
                {
                    SetKey(recordPointer, key);
                    SetValue(recordPointer, value);
                    SetDistance(recordPointer, distance);


#if DEBUG
                    Interlocked.Add(ref headerPointer->DistanceSum, distance - 1);
                    int maxDistance = headerPointer->MaxDistance;
                    if (maxDistance <= distance)
                    {
                        Interlocked.CompareExchange(ref headerPointer->MaxDistance, distance - 1, maxDistance);
                    }
#endif
                    Interlocked.Increment(ref headerPointer->RecordCount);
                    return;
                }
                else if (currentRecordDistance < distance) // found richer record
                {
                    /* Swap Robin Hood style */
                    TKey tempKey = GetKey(GetKeyPointer(recordPointer));
                    TValue tempValue = GetValue(GetValuePointer(recordPointer));

                    SetKey(recordPointer, key);
                    SetValue(recordPointer, value);
                    SetDistance(recordPointer, distance);
#if DEBUG
                    Interlocked.Add(ref headerPointer->DistanceSum, distance - currentRecordDistance);
#endif

                    key = tempKey;
                    value = tempValue;
                    distance = currentRecordDistance;
                }
                if (distance++ > maxAllowedDistance)
                {
                    ReachedMaxAllowedDistance();
                }
                recordPointer += recordSize;
                context.CurrentSlot++;
                if (recordPointer >= endTablePointer) // start from begining when reaching the end
                {
                    recordPointer = tablePointer;
                    context.CurrentSlot = 0L;
                }
            }
        }

        private void ReachedMaxAllowedDistance()
        {
            throw new InvalidOperationException("Reached MaxAllowedDistance");
        }

        public void Delete(TKey key)
        {
            if (!Remove(key))
            {
                throw new ArgumentException($"key {key} not found");
            }
        }

        public bool Remove(TKey key)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                byte* emptyRecordPointer = FindRecord(ref context);
                if (emptyRecordPointer == null)
                {
                    return false;
                }
#if DEBUG
                Interlocked.Add(ref headerPointer->DistanceSum, 1 - GetDistance(emptyRecordPointer));
#endif
                byte* currentRecordPointer = emptyRecordPointer;
                while (true)
                {
                    /*
                     * shift backward all entries following the entry to delete until either find an empty slot, 
                     * or a record with a distance of 0 (1 in our case)
                     * http://codecapsule.com/2013/11/17/robin-hood-hashing-backward-shift-deletion/
                     */
                    
                    currentRecordPointer += recordSize;
                    context.CurrentSlot++;
                    if (currentRecordPointer >= endTablePointer) // start from begining when reaching the end
                    {
                        currentRecordPointer = tablePointer;
                        context.CurrentSlot = 0;
                    }
                    LockIfNeeded(ref context);
                    short distance = GetDistance(currentRecordPointer);
                    if (distance <= 1)
                    {
                        SetDistance(emptyRecordPointer, 0);
                        Interlocked.Decrement(ref headerPointer->RecordCount);
                        return true;
                    }
                    SetKey(emptyRecordPointer, GetKey(currentRecordPointer));
                    SetValue(emptyRecordPointer, GetValue(currentRecordPointer));
                    SetDistance(emptyRecordPointer, (short)(distance - 1));
#if DEBUG
                    Interlocked.Decrement(ref headerPointer->DistanceSum);
#endif
                    emptyRecordPointer = currentRecordPointer;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        private unsafe struct NonBlockingOperationContext
        {
            public long InitialSlot;
            public long RemainingSlotsInChunk;
            public long CurrentSlot;
            public int* Versions;
            public int VersionIndex;
            public TKey Key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReadVersion(ref NonBlockingOperationContext context)
        {
            if (context.Versions == null) return;
            if (context.RemainingSlotsInChunk == 0)
            {
                var syncObject = syncObjects[context.CurrentSlot >> chunkBits];
                context.Versions[context.VersionIndex] = syncObject.Version;
                context.RemainingSlotsInChunk = chunkSize - (context.CurrentSlot & chunkMask) - 1;
                context.VersionIndex++;
            }
            else
            {
                context.RemainingSlotsInChunk--;
            }
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* FindRecordNonBlocking(ref NonBlockingOperationContext context)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            int distance = 1;
        loop:
            {
                ReadVersion(ref context);

                if (GetDistance(recordPointer) == 0) return null;
                if (comparer.Equals(context.Key, GetKey(GetKeyPointer(recordPointer))))
                {
                    return recordPointer;
                }
                recordPointer += recordSize;
                context.CurrentSlot++;
                if (recordPointer >= endTablePointer)
                {
                    recordPointer = tablePointer;
                    context.CurrentSlot = 0;
                }
                if (distance > maxAllowedDistance)
                {
                    ReachedMaxAllowedDistance();
                }
                distance++;
                goto loop;
            }
        }

        private unsafe struct OperationContext
        {
            public long InitialSlot;
            public long RemainingSlotsInChunk;
            public long CurrentSlot;
            public bool* TakenLocks;
            public int LockIndex;
            public TKey Key;
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* FindRecord(ref OperationContext context)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);

            int distance = 1;

            loop:
            {
                LockIfNeeded(ref context);

                if (GetDistance(recordPointer) == 0) return null;
                if (comparer.Equals(context.Key, GetKey(GetKeyPointer(recordPointer))))
                {
                    return recordPointer;
                }
                recordPointer += recordSize;
                context.CurrentSlot++;
                if (recordPointer >= endTablePointer)
                {
                    recordPointer = tablePointer;
                    context.CurrentSlot = 0;
                }
                if (distance > maxAllowedDistance)
                {
                    ReachedMaxAllowedDistance();
                }
                distance++;

                goto loop;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeTakenLocks(bool* takenLocks)
        {
            switch (maxLocksPerOperation)
            {
                case 0: return;
                case 2: *(short*)takenLocks = 0; return;
                case 3: *(short*)takenLocks = 0; takenLocks[2] = false; return;
                case 5: *(int*)takenLocks = 0; takenLocks[4] = false; return;
                case 9: *(long*)takenLocks = 0L; takenLocks[8] = false; return;
            }
            long* longPointer = (long*)takenLocks;
            long* endLongPointer = (long*)(takenLocks + maxLocksPerOperation - 1);
            while (longPointer < endLongPointer)
            {
                *longPointer = 0L;
                longPointer++;
            }
            *(bool*)endLongPointer = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LockIfNeeded(ref OperationContext context)
        {
            if (context.TakenLocks == null) return;
            if (context.RemainingSlotsInChunk == 0)
            {
                if (context.TakenLocks[context.LockIndex] == false)
                {
#if SPINLATCH
                    SpinLatch.Enter(ref syncObjects[context.CurrentSlot >> chunkBits],
                        ref context.TakenLocks[context.LockIndex]);
#else
                    var syncObject = syncObjects[context.CurrentSlot >> chunkBits];
                    Monitor.Enter(syncObject, 
                        ref context.TakenLocks[context.LockIndex]);
                    syncObject.IsWriterInProgress = true;
#endif
                }
                context.RemainingSlotsInChunk = chunkSize - (context.CurrentSlot & chunkMask) - 1;
                context.LockIndex++;
            }
            else
            {
                context.RemainingSlotsInChunk--;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReleaseLocks(ref OperationContext context)
        {
            if (context.TakenLocks == null) return;
            int chunkIndex = (int) (context.InitialSlot >> chunkBits);
            for (int lockIndex = 0; lockIndex < maxLocksPerOperation; lockIndex++ )
            {
                if (context.TakenLocks[lockIndex] == false) return;
#if SPINLATCH
                SpinLatch.Exit(ref syncObjects[chunkIndex]);
#else
                var syncObject = syncObjects[chunkIndex];
                unchecked { syncObject.Version++; }
                syncObject.IsWriterInProgress = false;
                Monitor.Exit(syncObjects[chunkIndex]);
#endif

                chunkIndex++;
                if (chunkIndex >= syncObjects.Length) chunkIndex = 0;
            }
        }

        public void WarmUp()
        {
            mappingSession.WarmUp();
        }


        public bool IsDisposed { get; private set; }

        public ICollection<TKey> Keys => new StaticFixedSizeHashTableKeyCollection<TKey, TValue>(this);

        public ICollection<TValue> Values => new StaticFixedSizeHashTableValueCollection<TKey, TValue>(this);

        int ICollection<KeyValuePair<TKey, TValue>>.Count => (int) Count;

        public bool IsReadOnly => false;

        public TValue this[TKey key]
        {
            get
            {
                if (!TryGetValue(key, out TValue value))
                {
                    throw new ArgumentException($"Key {key} not found.");
                }
                return value;
            }
            set
            {
                Put(key, value);
            }
        }

        public void Dispose()
        {
            if (IsDisposed) return;
            IsDisposed = true;
            if (this.mappingSession != null) this.mappingSession.Dispose();
            if (this.memoryMapper != null) this.memoryMapper.Dispose();
        }

        public void Flush()
        {
            this.memoryMapper.Flush();
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            if (Count > 0)
            {
                Memory.ZeroMemory(tablePointer, recordSize * slotCount);
                Count = 0;
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out TValue value))
            {
                return valueComparer.Equals(item.Value, value);
            }
            return false;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "arrayIndex parameter must be greater than zero");
            if (this.Count > array.Length - arrayIndex) throw new ArgumentException("The array has not enough space to hold all items");
            foreach (var keyValue in this)
            {
                array[arrayIndex++] = keyValue;
            }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }


        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new StaticFixedSizeHashTableRecordEnumerator<TKey, TValue>(this);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
