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

        internal StaticHashTableConfig<TKey, TValue> config;
        readonly bool isAligned;
        private MemoryMapper memoryMapper;
        private MemoryMappingSession mappingSession;
        private byte* fileBaseAddress;
        public readonly ThreadSafety ThreadSafety;
        private int syncObjectCount;

        /// <summary>
        /// Max distance ever seen in the hash table. 
        /// MaxDistance is updated only on adding, It is not uptaded on removing.
        /// </summary>
        // MaxDistance starts with 0 while internal distance starts with 1. So, it is the real max distance.
        public int MaxDistance => config.HeaderPointer->MaxDistance;

        //Note that float casts are not redundant as VS says.
        public float LoadFactor => (float)Count / (float)config.SlotCount;
        public float MeanDistance => (float)config.HeaderPointer->DistanceSum / (float)Count;

        private readonly IEqualityComparer<TKey> comparer;
        private readonly IEqualityComparer<TValue> valueComparer;
        private readonly long Capacity;


        public long Count
        {
            get { return config.HeaderPointer->RecordCount; }
            private set { config.HeaderPointer->RecordCount = value; }
        }



        public StaticFixedSizeHashTable(string filePath, long capacity, ThreadSafety threadSafety, Func<TKey, long> hashFunction = null, IEqualityComparer<TKey> comparer = null,  bool isAligned = false)
        {
            this.isAligned = isAligned;
            config.KeyOffset = 0;
            this.config.HashFunction = hashFunction;
            this.comparer = comparer ?? EqualityComparer<TKey>.Default;
            this.valueComparer = EqualityComparer<TValue>.Default;
            CalculateOffsetsAndSizesDependingOnAlignement();
            config.SlotCount = Math.Max(capacity + capacity/8 + capacity/16, 4);
            config.SlotCount = Bits.IsPowerOfTwo(config.SlotCount) ? config.SlotCount : Bits.NextPowerOf2(config.SlotCount);
            this.Capacity = config.SlotCount - config.SlotCount/8 - capacity/16; // conservative max load factor = 81.25%
            config.HashMask = config.SlotCount - 1L;
            this.ThreadSafety = threadSafety;
            var fileSize = (long) sizeof(StaticFixedSizeHashTableFileHeader) +  config.SlotCount * config.RecordSize;
            fileSize += (Constants.AllocationGranularity - (fileSize & (Constants.AllocationGranularity - 1))) & (Constants.AllocationGranularity - 1);

            var isNew = !File.Exists(filePath);

            memoryMapper = new MemoryMapper(filePath, (long) fileSize);
            mappingSession = memoryMapper.OpenSession();

            fileBaseAddress = mappingSession.GetBaseAddress();
            config.HeaderPointer = (StaticFixedSizeHashTableFileHeader*)fileBaseAddress;


            if (isNew)
            {
                InitializeHeader();
            }
            else
            {
                ValidateHeader();
                config.SlotCount = config.HeaderPointer->SlotCount;
            }
            config.TablePointer = fileBaseAddress + sizeof(StaticFixedSizeHashTableFileHeader);
            config.EndTablePointer = config.TablePointer + config.RecordSize * config.SlotCount;
            config.MaxAllowedDistance = (short) Math.Max(config.SlotCount, 448);

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
            // with 8 locks and 64 slots per chunk, we can cover 8*64 = 512 slots and (8 - 1)* 64 = 448 config.MaxAllowedDistance  
            // Most of the time, when distance < 64, we only need to lock one sync object
            // Chunk size cannot be greater than the number of slots
            config.ChunkSize = Math.Min(Math.Max(config.SlotCount / syncObjectCount, 64), config.SlotCount);
            config.ChunkMask = config.ChunkSize - 1;
            config.ChunkBits = Bits.MostSignificantBit(config.ChunkSize);

            // As said, max locks per operation cannot be greater than 8. 
            // We must satisfy (MaxLocksPerOperation - 1) * ChunkSize >= MaxAllowedDistance,
            // We don't want to allow a a distance greater than 448. 
            // config.ChunkSize == 1 is an edge case.
            config.MaxAllowedDistance = config.ChunkSize == 1 ? (short)8: (short) Math.Min(7 * config.ChunkSize, 448);

            // We use an array of max locks per operation booleans to keep track of locked sync objects
            config.MaxLocksPerOperation = config.ChunkSize == 1 ?  8 : Math.Max((int)(config.MaxAllowedDistance / config.ChunkSize + 1L), 2);

            // recalc with constraints applied 
            syncObjectCount = (int)(config.SlotCount / config.ChunkSize);

            Debug.Assert(Bits.IsPowerOfTwo(config.ChunkSize));
            Debug.Assert((config.MaxLocksPerOperation > 1 || config.ChunkSize == 1 && config.MaxLocksPerOperation == 1) && config.MaxLocksPerOperation <= 8);
            Debug.Assert(((config.MaxLocksPerOperation - 1) * config.ChunkSize >= config.MaxAllowedDistance) || config.ChunkSize == 1);


           
#if SPINLATCH
            config.SyncObjects = new int[syncObjectCount];
            for (int i = 0; i < syncObjectCount; i++) config.SyncObjects[i] = 0;
#else
            config.SyncObjects = new SyncObject[syncObjectCount];
            for (int i = 0; i < syncObjectCount; i++) config.SyncObjects[i] = new SyncObject();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(long slotIndex) => 
            config.TablePointer + config.RecordSize * slotIndex;

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
            *(short*)(recordPointer + config.DistanceOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetDistance(byte* recordPointer, short distance)
        {
            *(short*)(recordPointer + config.DistanceOffset) = distance;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetKey(byte* recordPointer, TKey key)
        {
            *(TKey*)(recordPointer + config.KeyOffset) = key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetValue(byte* recordPointer, TValue value)
        {
            *(TValue*)(recordPointer + config.ValueOffset) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetKeyPointer(byte* recordPointer) => 
            recordPointer + config.KeyOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetKeyAsSpan(byte* keyPointer) => 
            new ReadOnlySpan<byte>(keyPointer, config.KeySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TKey GetKey(byte* keyPointer) => 
            *(TKey*)keyPointer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetValuePointer(byte* recordPointer) => 
            recordPointer + config.ValueOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetValueAsSpan(byte *valuePointer) => 
            new ReadOnlySpan<byte>(valuePointer, (int)config.ValueSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static internal TValue GetValue(byte* valuePointer) => 
            *(TValue*)valuePointer;

        void InitializeHeader()
        {
            config.HeaderPointer->IsAligned = isAligned;
            config.HeaderPointer->DistanceSum = 0;
            config.HeaderPointer->KeySize = config.KeySize;
            config.HeaderPointer->Magic = StaticFixedSizeHashTableFileHeader.MagicNumber;
            config.HeaderPointer->MaxDistance = 0;
            config.HeaderPointer->RecordCount = 0;
            config.HeaderPointer->RecordSize = config.RecordSize;
            config.HeaderPointer->Reserved[0] = 0;
            config.HeaderPointer->SlotCount = config.SlotCount;
            config.HeaderPointer->ValueSize = config.ValueSize;
        }

        void ValidateHeader()
        {
            if ( config.HeaderPointer->Magic != StaticFixedSizeHashTableFileHeader.MagicNumber)
            {
                throw new FormatException($"This is not a {nameof(StaticFixedSizeHashTable<TKey, TValue>)} file");
            }
            if (config.HeaderPointer->IsAligned != isAligned)
            {
                throw new ArgumentException("Mismatched alignement");
            }
            if (config.HeaderPointer->KeySize != config.KeySize)
            {
                throw new ArgumentException("Mismatched config.KeySize");
            }
            if (config.HeaderPointer->ValueSize != config.ValueSize)
            {
                throw new ArgumentException("Mismatched ValueSize");
            }
            if (config.HeaderPointer->RecordSize != config.RecordSize)
            {
                throw new ArgumentException("Mismatched SlotSize");
            }
        }

        private void CalculateOffsetsAndSizesDependingOnAlignement()
        {
            config.KeySize = sizeof(TKey);
            config.ValueSize = sizeof(TValue);
            config.DistanceSize = sizeof(short);


            int keyAlignement = GetAlignement(config.KeySize);
            int valueAlignement = GetAlignement(config.ValueSize);
            int distanceAlignement = GetAlignement(config.DistanceSize);
            int slotAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            //config.KeyOffset = 0;
            config.ValueOffset = config.KeyOffset + config.KeySize + GetPadding(config.KeyOffset + config.KeySize, valueAlignement);
            config.DistanceOffset = config.ValueOffset + config.ValueSize + GetPadding(config.ValueOffset + config.ValueSize, distanceAlignement);
            config.RecordSize = config.DistanceOffset + config.DistanceSize + GetPadding(config.DistanceOffset + config.DistanceSize, slotAlignement);
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
            if (config.HashFunction == null)
            {
                return Hashing.FastHash64((byte*)&key, config.KeySize) & config.HashMask;
            }
            else
            {
                //return config.HashFunction(key) * 11400714819323198485LU & mask;
                return config.HashFunction(key) & config.HashMask;
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

        public bool TryGetValue(TKey key, out ReadOnlySpan<byte> span)
        {
            span = new Span<byte>(new byte[10]);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsNonBlockingOperationValid(ref NonBlockingOperationContext context)
        {
            if (context.Versions == null) return true;
            int chunkIndex = (int)(context.InitialSlot >> config.ChunkBits);
            
            for (int i=0; i <= context.VersionIndex; i++)
            {
                var syncObject = config.SyncObjects[chunkIndex];
                if (syncObject.IsWriterInProgress || syncObject.Version != context.Versions[i])
                {
                    return false;
                }
                chunkIndex++;
                if (chunkIndex >= config.SyncObjects.Length) chunkIndex = 0;
            }
            return true;
        }

        public bool TryGetValueNonBlocking(TKey key, out TValue value)
        {
            int* versions = stackalloc int[config.MaxLocksPerOperation];
            var context = new NonBlockingOperationContext
            {
                Versions = versions,
                InitialSlot = GetInitialSlot(key),
                Key = key
            };
            bool isFound;
            while (true)
            {
                var recordPointer = FindRecordNonBlocking(ref context);
                isFound = recordPointer != null;
                value = recordPointer ==  null ? default : GetValue(GetValuePointer(recordPointer));
                if (IsNonBlockingOperationValid(ref context)) return isFound;
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

        public TValue GetOrAdd(TKey key, TValue value)
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
                return value;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    var value = valueFactory(key);
                    RobinHoodAdd(ref context, value);
                    return value;
                }
                return GetValue(GetValuePointer(recordPointer));
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public TValue GetOrAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    var value = valueFactory(key, factoryArgument);
                    RobinHoodAdd(ref context, value);
                    return value;
                }
                return GetValue(GetValuePointer(recordPointer));
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                byte* emptyRecordPointer = FindRecord(ref context);
                if (emptyRecordPointer == null)
                {
                    value = default;
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

                    currentRecordPointer += config.RecordSize;
                    context.CurrentSlot++;
                    if (currentRecordPointer >= config.EndTablePointer) // start from begining when reaching the end
                    {
                        currentRecordPointer = config.TablePointer;
                        context.CurrentSlot = 0;
                    }
                    LockIfNeeded(ref context);
                    short distance = GetDistance(currentRecordPointer);
                    if (distance <= 1)
                    {
                        SetDistance(emptyRecordPointer, 0);
                        value = GetValue(GetValuePointer(currentRecordPointer));
                        Interlocked.Decrement(ref config.HeaderPointer->RecordCount);
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

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    return false;
                }
                else
                {
                    var value = GetValue(GetValuePointer(recordPointer));
                    if (valueComparer.Equals(value, comparisonValue))
                    {
                        SetValue(recordPointer, newValue);
                        return true;
                    }
                    return false;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    RobinHoodAdd(ref context, addValue);
                    return addValue;
                }
                else
                {
                    TValue value = GetValue(GetValuePointer(recordPointer));
                    TValue newValue = updateValueFactory(key, value);
                    SetValue(recordPointer, newValue);
                    return newValue;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks);
            try
            {
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    var value = addValueFactory(key);
                    RobinHoodAdd(ref context, value);
                    return value;
                }
                else
                {
                    var value = GetValue(GetValuePointer(recordPointer));
                    var newValue = updateValueFactory(key, value);
                    SetValue(recordPointer, newValue);
                    return newValue;
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
                    Interlocked.Add(ref config.HeaderPointer->DistanceSum, distance - 1);
                    int maxDistance = config.HeaderPointer->MaxDistance;
                    if (maxDistance <= distance)
                    {
                        Interlocked.CompareExchange(ref config.HeaderPointer->MaxDistance, distance - 1, maxDistance);
                    }
#endif
                    Interlocked.Increment(ref config.HeaderPointer->RecordCount);
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
                    Interlocked.Add(ref config.HeaderPointer->DistanceSum, distance - currentRecordDistance);
#endif

                    key = tempKey;
                    value = tempValue;
                    distance = currentRecordDistance;
                }
                if (distance++ > config.MaxAllowedDistance)
                {
                    ReachedMaxAllowedDistance();
                }
                recordPointer += config.RecordSize;
                context.CurrentSlot++;
                if (recordPointer >= config.EndTablePointer) // start from begining when reaching the end
                {
                    recordPointer = config.TablePointer;
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
                Interlocked.Add(ref config.HeaderPointer->DistanceSum, 1 - GetDistance(emptyRecordPointer));
#endif
                byte* currentRecordPointer = emptyRecordPointer;
                while (true)
                {
                    /*
                     * shift backward all entries following the entry to delete until either find an empty slot, 
                     * or a record with a distance of 0 (1 in our case)
                     * http://codecapsule.com/2013/11/17/robin-hood-hashing-backward-shift-deletion/
                     */
                    
                    currentRecordPointer += config.RecordSize;
                    context.CurrentSlot++;
                    if (currentRecordPointer >= config.EndTablePointer) // start from begining when reaching the end
                    {
                        currentRecordPointer = config.TablePointer;
                        context.CurrentSlot = 0;
                    }
                    LockIfNeeded(ref context);
                    short distance = GetDistance(currentRecordPointer);
                    if (distance <= 1)
                    {
                        SetDistance(emptyRecordPointer, 0);
                        Interlocked.Decrement(ref config.HeaderPointer->RecordCount);
                        return true;
                    }
                    SetKey(emptyRecordPointer, GetKey(currentRecordPointer));
                    SetValue(emptyRecordPointer, GetValue(currentRecordPointer));
                    SetDistance(emptyRecordPointer, (short)(distance - 1));
#if DEBUG
                    Interlocked.Decrement(ref config.HeaderPointer->DistanceSum);
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
            public int* Versions;
            public int VersionIndex;
            public TKey Key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* FindRecordNonBlocking(ref NonBlockingOperationContext context)
        {
            var spinWait = new SpinWait();
        start:
            var currentSlot = context.InitialSlot;
            var chunkIndex = (int)(currentSlot >> config.ChunkBits);
            var syncObject = config.SyncObjects[chunkIndex];
            if (syncObject.IsWriterInProgress)
            {
                spinWait.SpinOnce();
                goto start;
            }
            var remainingSlotsInChunk = config.ChunkSize - (context.InitialSlot & config.ChunkMask);
            context.VersionIndex = 0;
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            int distance = 1;
            context.Versions[0] = syncObject.Version;
        
            while (true)
            {
                if (GetDistance(recordPointer) == 0) return null;
                if (comparer.Equals(context.Key, GetKey(GetKeyPointer(recordPointer)))) return recordPointer;
                recordPointer += config.RecordSize;
                if (recordPointer >= config.EndTablePointer) recordPointer = config.TablePointer;
                if (distance > config.MaxAllowedDistance) ReachedMaxAllowedDistance();
                distance++;
                if (--remainingSlotsInChunk == 0)
                {
                    context.VersionIndex++;
                    chunkIndex++;
                    if (chunkIndex >= config.SyncObjects.Length) chunkIndex = 0;
                    syncObject = config.SyncObjects[chunkIndex];
                    if (syncObject.IsWriterInProgress)
                    {
                        spinWait.SpinOnce();
                        goto start;
                    }
                    remainingSlotsInChunk = config.ChunkSize;
                    context.Versions[context.VersionIndex] = syncObject.Version;
                }
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
                recordPointer += config.RecordSize;
                context.CurrentSlot++;
                if (recordPointer >= config.EndTablePointer)
                {
                    recordPointer = config.TablePointer;
                    context.CurrentSlot = 0;
                }
                if (distance > config.MaxAllowedDistance)
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
            switch (config.MaxLocksPerOperation)
            {
                case 0: return;
                case 2: *(short*)takenLocks = 0; return;
                case 3: *(short*)takenLocks = 0; takenLocks[2] = false; return;
                case 5: *(int*)takenLocks = 0; takenLocks[4] = false; return;
                case 9: *(long*)takenLocks = 0L; takenLocks[8] = false; return;
            }
            long* longPointer = (long*)takenLocks;
            long* endLongPointer = (long*)(takenLocks + config.MaxLocksPerOperation - 1);
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
                    SpinLatch.Enter(ref config.SyncObjects[context.CurrentSlot >> config.ChunkBits],
                        ref context.TakenLocks[context.LockIndex]);
#else
                    var syncObject = config.SyncObjects[context.CurrentSlot >> config.ChunkBits];
                    Monitor.Enter(syncObject, 
                        ref context.TakenLocks[context.LockIndex]);
                    syncObject.IsWriterInProgress = true;
#endif
                }
                context.RemainingSlotsInChunk = config.ChunkSize - (context.CurrentSlot & config.ChunkMask) - 1;
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
            int chunkIndex = (int) (context.InitialSlot >> config.ChunkBits);
            for (int lockIndex = 0; lockIndex < config.MaxLocksPerOperation; lockIndex++ )
            {
                if (context.TakenLocks[lockIndex] == false) return;
#if SPINLATCH
                SpinLatch.Exit(ref config.SyncObjects[chunkIndex]);
#else
                var syncObject = config.SyncObjects[chunkIndex];
                unchecked { syncObject.Version++; }
                syncObject.IsWriterInProgress = false;
                Monitor.Exit(config.SyncObjects[chunkIndex]);
#endif

                chunkIndex++;
                if (chunkIndex >= config.SyncObjects.Length) chunkIndex = 0;
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
                Memory.ZeroMemory(config.TablePointer, config.RecordSize * config.SlotCount);
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
