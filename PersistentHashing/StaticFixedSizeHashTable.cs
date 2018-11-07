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
    public unsafe class StaticFixedSizeHashTable<TKey, TValue>: IDisposable, IDictionary<TKey, TValue> where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><record-padding>

        internal StaticHashTableConfig<TKey, TValue> config;

        /// <summary>
        /// Max distance ever seen in the hash table. 
        /// MaxDistance is updated only on adding, It is not uptaded on removing.
        /// It' only updated when building with DEBUG simbol defined.
        /// </summary>
        // MaxDistance starts with 0 while internal distance starts with 1. So, it is the real max distance.
        public int MaxDistance => config.HeaderPointer->MaxDistance;

        //Note that float casts are not redundant as VS says.
        public float LoadFactor => (float)Count / (float)config.SlotCount;

        // this is only updated when building with DEBUG simbol defined.
        public float MeanDistance => (float)config.HeaderPointer->DistanceSum / (float)Count;

        public long Capacity => config.Capacity;

        public long Count => config.HeaderPointer->RecordCount;

        private readonly MemoryMappingSession dataSession;
        internal volatile byte* dataBaseAddress;

        internal StaticFixedSizeHashTable(ref StaticHashTableConfig<TKey, TValue> config)
        {
            this.config = config;
            if (config.DataFile != null)
            {
                dataSession = config.DataFile.OpenSession();
                dataSession.BaseAddressChanged += DataBaseAddressChanged;
            }
        }

        private void DataBaseAddressChanged(object sender, MemoryMappingSession.BaseAddressChangedEventArgs e)
        {
            dataBaseAddress = e.BaseAddress;
        }

        public StaticFixedSizeHashTable(string filePath, long capacity, Func<TKey, long> hashFunction = null, IEqualityComparer<TKey> keyComparer = null,  bool isAligned = false)
        {
            config.IsAligned = isAligned;
            config.KeyOffset = 0;
            this.config.HashFunction = hashFunction;
            this.config.KeyComparer = config.KeyComparer ?? EqualityComparer<TKey>.Default;
            this.config.ValueComparer = EqualityComparer<TValue>.Default;
            CalculateOffsetsAndSizesDependingOnAlignement();

            // SlotCount 1, 2, 4, and 8 are edge cases that is not worth to support. So min slot count is 16, it doesn't show "anomalies".
            // (capacity + 6) / 7 is Ceil(capacity FloatDiv 7)
            // we want a MaxLoadFactor = Capacity/SlotCount = 87.5% this is why we set SlotCount = capacity + capacity/7 => 7 * slotCount = 8 * capacity => capacity/SlotCount = 7/8 = 87.5%
            config.SlotCount = Math.Max(capacity + (capacity + 6)/7, 16);

            config.SlotCount = Bits.IsPowerOfTwo(config.SlotCount) ? config.SlotCount : Bits.NextPowerOf2(config.SlotCount);
            this.config.Capacity = (config.SlotCount / 8) * 7; // max load factor = 7/8 = 87.5%
            config.HashMask = config.SlotCount - 1L;

            long fileSize = (long) sizeof(StaticFixedSizeHashTableFileHeader) +  config.SlotCount * config.RecordSize;
            fileSize += (Constants.AllocationGranularity - (fileSize & Constants.AllocationGranularityMask)) & Constants.AllocationGranularityMask;

            bool isNew = !File.Exists(filePath);

            config.TableMemoryMapper = new MemoryMapper(filePath, (long) fileSize);
            config.TableMappingSession = config.TableMemoryMapper.OpenSession();

            config.TableFileBaseAddress = config.TableMappingSession.GetBaseAddress();
            config.HeaderPointer = (StaticFixedSizeHashTableFileHeader*)config.TableFileBaseAddress;


            if (isNew)
            {
                InitializeHeader();
            }
            else
            {
                ValidateHeader();
                config.SlotCount = config.HeaderPointer->SlotCount;
            }
            config.TablePointer = config.TableFileBaseAddress + sizeof(StaticFixedSizeHashTableFileHeader);
            config.EndTablePointer = config.TablePointer + config.RecordSize * config.SlotCount;
            config.SlotBits = Bits.MostSignificantBit(config.SlotCount);

            InitializeSynchronization();
        }

        private void InitializeSynchronization()
        {
            /*
             * We use System.Threading.Monitor To achieve synchronization
             * The table is divided into equal sized chunks of slots.
             * The numer of chunks is a power of two that ranges from 8 to 8192
             * We use an array of sync objects with one sync object per chunk.
             * The sync object associated with a chunk is locked when accesing slots in the chunk.
             * If the record distance is greater than chunk size, more than one sync object will be locked.
             * But we never lock more than 8 sync objects in a single operation.
             */

           
            /*
             According to the Birthday problem and using the Square approximation
             p(n) = n^2/2/m. where p is the probalitity that at least two people have the same birthday, 
             n is the number of people and m is the number of days in a year.
             m = n^2/2/p(n)
             ChunkCount which is equals to SyncObjects.Length maps to the number of days in a year (m), 
             numer of threads accessing hash table maps to number of people (n).
             we are guessing number of threads = Environment.ProcessorCount * 2  
             and we want a probability of 1/8 that a thread gets blocked.
            
            */
            config.ChunkCount = Math.Min(Environment.ProcessorCount * Environment.ProcessorCount * 16, 8192);

            // but we need a power of two
            if (Bits.IsPowerOfTwo(config.ChunkCount) == false) config.ChunkCount = Math.Min(Bits.NextPowerOf2(config.ChunkCount), 8192);


            // We impose the following constraint: max locks per operation cannot be greater than 8
            // We use one long as an array of 8 bools to keep track of locked sync objects
            // we want at least 64 slots per chunk when SlotCount >= 512, for smaller tables SlotCount/8 slots per chunk
            // with 8 locks and 64 slots per chunk, we can cover 8*64 = 512 slots and (8 - 1)* 64 = 448 MaxAllowedDistance  
            // Most of the time, when distance < 64, we only need to lock one sync object
            int minChunkSize = (int) Math.Min(config.SlotCount / 8, 64);
            config.ChunkSize = Math.Max(config.SlotCount / config.ChunkCount, minChunkSize);
            config.ChunkMask = config.ChunkSize - 1;
            config.ChunkBits = Bits.MostSignificantBit(config.ChunkSize);

            // recalc with constraints applied 
            config.ChunkCount = (int)(config.SlotCount / config.ChunkSize);

            // As said, max locks per operation cannot be greater than 8. 
            // We must satisfy (MaxLocksPerOperation - 1) * ChunkSize >= MaxAllowedDistance,
            // Threrefore MaxAllowedDistance cannot be greater than 7 * ChunkSize 
            // But this value can be huge, So we want to constraint MaxAllowedDistance to a smaller value.
            // It is known that max distance grows very slowly (logarithmicaly) with slot count. We guess: max distance = a + k * log2 (slotCount)
            // (4.3 In Robin Hood hashing, the maximum DIB increases with the table size: http://codecapsule.com/2014/05/07/implementing-a-key-value-store-part-6-open-addressing-hash-tables/)
            // for SlotCount = 512, the maximum MaxAllowedDistance without deadlocks is (config.ChunkCount - 2) * ChunkSize = 6*64 = 384.
            // Doing the math we get a=6, k=42, log2(slotCount) = config.SlotCountBits. 
            // 42 is "The Answer to the Ultimate Question of Life, the Universe, and Everything". Is it just coincidence?
            config.MaxAllowedDistance = (int) Math.Min(7 * config.ChunkSize, 6 + 42 * config.SlotBits);

            // We nedd to adjust MaxAllowedDistance to avoid deadlocks. A deadlock will occur when a thread A start locking the first chunk,
            // another thread B start locking the last chunk, then B tries to lock the first chunk. If A reaches the
            // last chunk we have a dealock. Threfore MaxAllowedDistance must not span more than config.ChunkCount - 2 chunks.
            config.MaxAllowedDistance = (int) Math.Min((config.ChunkCount - 2) * config.ChunkSize, config.MaxAllowedDistance);


            // We use an array of max locks per operation booleans to keep track of locked sync objects
            // (config.MaxAllowedDistance >> config.ChunkBits) + ((config.MaxAllowedDistance & config.ChunkMask) == 0 ? 1 : 2) this weird thing is ceil(MaxAllowedDistance FloatDiv ChunkSize) + 1
            config.MaxLocksPerOperation = Math.Max((int)((config.MaxAllowedDistance >> config.ChunkBits) + ((config.MaxAllowedDistance & config.ChunkMask) == 0 ? 1 : 2)), 2);

            Debug.Assert(Bits.IsPowerOfTwo(config.ChunkSize));
            Debug.Assert(config.MaxLocksPerOperation > 1 && config.MaxLocksPerOperation <= 8);

            // MaxAllowedDistance is covered with MaxLocksPerOperation locks
            Debug.Assert((config.MaxLocksPerOperation - 1) * config.ChunkSize >= config.MaxAllowedDistance);

            // MaxAllowedDistance is reached before deadlocking.
            Debug.Assert(config.MaxAllowedDistance <= (config.ChunkCount - 2) * config.ChunkSize);

            config.SyncObjects = new SyncObject[config.ChunkCount];
            for (int i = 0; i < config.ChunkCount; i++) config.SyncObjects[i] = new SyncObject();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(long slot) => 
            config.TablePointer + config.RecordSize * slot;

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
        internal void SetValue(byte* recordPointer, TValue value)
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
            config.HeaderPointer->IsAligned = config.IsAligned;
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
            if (config.HeaderPointer->IsAligned != config.IsAligned)
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
            if (!config.IsAligned) return 0;
            int alignementMinusOne = alignement - 1;
            // (alignement - offsetWithoutPadding % alignement) % alignement
            return (alignement - (offsetWithoutPadding & alignementMinusOne)) & alignementMinusOne;
        }

        private int GetAlignement(int size)
        {
            if (!config.IsAligned) return 1;
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
        internal void InitializeOperationContext(ref OperationContext context, TKey key, void* takenLocks, bool isWriting)
        {
            context.CurrentSlot = context.InitialSlot = GetInitialSlot(key);
            context.Key = key;
            context.TakenLocks = (bool*)takenLocks;
            context.IsWriting = isWriting;
        }

        public bool ContainsKey(TKey key)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: false);
            try
            {
                return FindRecord(ref context) != null;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, false);
            try
            {
                byte* recordPointer = FindRecord(ref context);
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
                byte* recordPointer = FindRecordNonBlocking(ref context);
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


        internal long WriteToDataFile(ReadOnlySpan<byte> value)
        {
            int bytesToAllocateAndCopy = value.Length + sizeof(int);
            var valueOffset = this.config.DataFile.Allocate(bytesToAllocateAndCopy);
            byte* destination = dataBaseAddress + valueOffset;
            *(int*)destination = value.Length;
            var destinationSpan = new Span<byte>(destination + sizeof(int), value.Length);
            value.CopyTo(destinationSpan);
            return valueOffset;
        }

        internal bool TryAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument, out TValue existingOrAddedValue)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null) // not found
                {
                    existingOrAddedValue = valueFactory(key, factoryArgument); // added value
                    RobinHoodAdd(ref context, existingOrAddedValue);
                    return true;
                }
                existingOrAddedValue = GetValue(GetValuePointer(recordPointer)); // existing value
                return false;
            }
            finally
            {
                ReleaseLocks(ref context); // taken by FindRecord and RobinHoodAdd
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static TValue IdentityValueFactory(TKey key, TValue value)
        {
            return value;
        }
        static readonly Func<TKey, TValue, TValue> IdentityValueFactoryFunction = IdentityValueFactory;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static TValue NestedValueFactory(TKey key, Func<TKey, TValue> valueFactory)
        {
            return valueFactory(key);
        }
        static readonly Func<TKey, Func<TKey, TValue>, TValue> NestedValueFactoryFunction = NestedValueFactory;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(TKey key, TValue value)
        {
            // Trade-off: Performance vs DRY. The WET a more performant version won this time.

            //DRY version:
            //return TryAdd(key, IdentityValueFactoryFunction, value, out TValue existingOrAddedValue);


            // this is the WET and faster version:
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null) // not found
                {
                    RobinHoodAdd(ref context, value);
                    return true;
                }
                return false;
            }
            finally
            {
                ReleaseLocks(ref context); // taken by FindRecord and RobinHoodAdd
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            // The DRY version won this time. Writing everything twice is tiring and boring :-)
            return TryAdd(key, NestedValueFactoryFunction, valueFactory, out TValue existingOrAddedValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
        {
            return TryAdd(key, valueFactory, factoryArgument, out TValue existingOrAddedValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(TKey key, TValue value)
        {
            //TODO: PERF consider the WET version
            return GetOrAdd(key, IdentityValueFactoryFunction, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            //TODO: PERF consider the WET version
            return GetOrAdd(key, NestedValueFactoryFunction, valueFactory);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
        {
            TryAdd(key, valueFactory, factoryArgument, out TValue value);
            return value;
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* emptyRecordPointer = FindRecord(ref context);
                if (emptyRecordPointer == null)
                {
                    value = default;
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
                        value = GetValue(GetValuePointer(currentRecordPointer));
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Remove(TKey key)
        {
            return TryRemove(key, out TValue removedValue);
        }

        public void Put(TKey key, TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
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
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    return false;
                }
                else
                {
                    var value = GetValue(GetValuePointer(recordPointer));
                    if (config.ValueComparer.Equals(value, comparisonValue))
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


        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    TValue value = addValueFactory(key);
                    RobinHoodAdd(ref context, value);
                    return value;
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

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            // implementing this method with one single line of code is tempting, 
            // but it allocates a new delegate instance each time that has to be called.
            // return AddOrUpdate(key, _ => addValue, updateValueFactory);

            // Prefer the WET version, no allocations and no delegate calls.
            long takenLocks = 0L;
            var context = new OperationContext();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
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
                    TValue existingValue = GetValue(GetValuePointer(recordPointer));
                    TValue newValue = updateValueFactory(key, existingValue);
                    SetValue(recordPointer, newValue);
                    return newValue;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }


        internal void RobinHoodAdd(ref OperationContext context,  TValue value)
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
                if (distance > config.MaxAllowedDistance)
                {
                    ReachedMaxAllowedDistance();
                }
                distance++;
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
            throw new InvalidOperationException($"Reached MaxAllowedDistance {config.MaxAllowedDistance}");
        }

        public void Delete(TKey key)
        {
            if (!Remove(key))
            {
                throw new ArgumentException($"key {key} not found");
            }
        }

        private unsafe struct NonBlockingOperationContext
        {
            public long InitialSlot;
            public int* Versions;
            public int VersionIndex;
            public TKey Key;
        }

        private byte* FindRecordNonBlocking(ref NonBlockingOperationContext context)
        {
            var spinWait = new SpinWait();

        start:
            long currentSlot = context.InitialSlot;
            int chunkIndex = (int)(currentSlot >> config.ChunkBits);
            var syncObject = config.SyncObjects[chunkIndex];
            if (syncObject.IsWriterInProgress)
            {
                spinWait.SpinOnce();
                goto start;
            }
            long remainingSlotsInChunk = config.ChunkSize - (context.InitialSlot & config.ChunkMask);
            context.VersionIndex = 0;
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            int distance = 1;
            context.Versions[0] = syncObject.Version;
        
            while (true)
            {
                if (GetDistance(recordPointer) == 0) return null;
                if (config.KeyComparer.Equals(context.Key, GetKey(GetKeyPointer(recordPointer)))) return recordPointer;
                if (distance > config.MaxAllowedDistance) ReachedMaxAllowedDistance();
                distance++;
                recordPointer += config.RecordSize;
                if (recordPointer >= config.EndTablePointer) recordPointer = config.TablePointer;
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

        internal unsafe struct OperationContext
        {
            public long InitialSlot;
            public long RemainingSlotsInChunk;
            public long CurrentSlot;
            public TKey Key;
            public int LockIndex;
            public bool* TakenLocks;
            public bool IsWriting;
        }

        internal byte* FindRecord(ref OperationContext context)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            int distance = 1;

            while(true)
            {
                LockIfNeeded(ref context);

                if (GetDistance(recordPointer) == 0) return null;
                if (config.KeyComparer.Equals(context.Key, GetKey(GetKeyPointer(recordPointer))))
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
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LockIfNeeded(ref OperationContext context)
        {
            if (context.RemainingSlotsInChunk == 0)
            {
                if (context.TakenLocks[context.LockIndex] == false)
                {
                    SyncObject syncObject = config.SyncObjects[context.CurrentSlot >> config.ChunkBits];
#if SPINLATCH
                    SpinLatch.Enter(ref syncObject.Locked, ref context.TakenLocks[context.LockIndex]);
#else
                    Monitor.Enter(syncObject, ref context.TakenLocks[context.LockIndex]);
#endif
                    syncObject.IsWriterInProgress = context.IsWriting;
                }
                context.RemainingSlotsInChunk = config.ChunkSize - (context.CurrentSlot & config.ChunkMask);
                context.LockIndex++;
            }
            context.RemainingSlotsInChunk--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseLocks(ref OperationContext context)
        {
            int chunkIndex = (int) (context.InitialSlot >> config.ChunkBits);
            /*
             * Don't try  optimize like this:
             * 
             * for (int lockIndex = 0; lockIndex < config.LockIndex; lockIndex++)
             *    remove line:  if (context.TakenLocks[lockIndex] == false) return;
             *    
             * Because it doesn't guarantee all locks are released when ThreadAbortException is thrown in LockIfNeeded.
             */
            for (int lockIndex = 0; lockIndex < config.MaxLocksPerOperation; lockIndex++ )
            {
                if (context.TakenLocks[lockIndex] == false) return;
                var syncObject = config.SyncObjects[chunkIndex];
                if (context.IsWriting)
                {
                    unchecked { syncObject.Version++; }
                    syncObject.IsWriterInProgress = false;
                }
#if SPINLATCH
                SpinLatch.Exit(ref syncObject.Locked);
#else
                Monitor.Exit(syncObject);
#endif
                chunkIndex++;
                if (chunkIndex >= config.ChunkCount) chunkIndex = 0;
            }
        }

        public void WarmUp()
        {
            config.TableMappingSession.WarmUp();
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
            if (this.config.TableMappingSession != null) this.config.TableMappingSession.Dispose();
            if (this.config.TableMemoryMapper != null) this.config.TableMemoryMapper.Dispose();
        }

        public void Flush()
        {
            config.TableMemoryMapper.Flush();
            config.DataFile?.Flush();
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            if (config.HeaderPointer->RecordCount > 0)
            {
                Memory.ZeroMemory(config.TablePointer, config.RecordSize * config.SlotCount);
                config.HeaderPointer->RecordCount = 0;
                config.HeaderPointer->MaxDistance = 0;
                config.HeaderPointer->DistanceSum = 0;
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out TValue value))
            {
                return config.ValueComparer.Equals(item.Value, value);
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
