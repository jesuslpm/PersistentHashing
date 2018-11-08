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

    public delegate ReadOnlySpan<byte> ValueFactory<in TKey>(TKey key);
    public delegate ReadOnlySpan<byte> ValueFactory<in TKey, in TArg>(TKey key, TArg arg);
    public delegate ReadOnlySpan<byte> UpdateValueFactory<in TKey>(TKey key, ReadOnlySpan<byte> value);



    public unsafe sealed class StaticConcurrentFixedKeySizeHashTable<TKey> where TKey:unmanaged
    {
        // <key><value-offset-padding><value-offset><distance padding><distance16><record-padding>

        internal readonly StaticHashTableConfig<TKey, long> config;

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
        private volatile byte* dataBaseAddress;


        internal StaticConcurrentFixedKeySizeHashTable(in StaticHashTableConfig<TKey, long> config)
        {
            this.config = config;
            dataSession = config.DataFile.OpenSession();
            dataSession.BaseAddressChanged += DataSession_BaseAddressChanged;
            dataBaseAddress = dataSession.GetBaseAddress();
        }

        private void DataSession_BaseAddressChanged(object sender, MemoryMappingSession.BaseAddressChangedEventArgs e)
        {
            dataBaseAddress = e.BaseAddress;
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
        void SetValueOffset(byte* recordPointer, long valueOffset)
        {
            *(long*)(recordPointer + config.ValueOffset) = valueOffset;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetKeyPointer(byte* recordPointer) =>
            recordPointer + config.KeyOffset;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TKey GetKey(byte* keyPointer) =>
            *(TKey*)keyPointer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetValueOffsetPointer(byte* recordPointer) =>
            recordPointer + config.ValueOffset;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static internal long GetValueOffset(byte* valueOffsetPointer) =>
            *(long*)valueOffsetPointer;


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
        internal void InitializeOperationContext(ref OperationContext<TKey> context, TKey key, void* takenLocks, bool isWriting)
        {
            context.CurrentSlot = context.InitialSlot = GetInitialSlot(key);
            context.Key = key;
            context.TakenLocks = (bool*)takenLocks;
            context.IsWriting = isWriting;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LockIfNeeded(ref OperationContext<TKey> context)
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

        private void ReachedMaxAllowedDistance()
        {
            throw new InvalidOperationException($"Reached MaxAllowedDistance {config.MaxAllowedDistance}");
        }

        internal byte* FindRecord(ref OperationContext<TKey> context)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            int distance = 1;

            while (true)
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
        internal void ReleaseLocks(ref OperationContext<TKey> context)
        {
            int chunkIndex = (int)(context.InitialSlot >> config.ChunkBits);
            /*
             * Don't try  optimize like this:
             * 
             * for (int lockIndex = 0; lockIndex < config.LockIndex; lockIndex++)
             *    remove line:  if (context.TakenLocks[lockIndex] == false) return;
             *    
             * Because it doesn't guarantee all locks are released when ThreadAbortException is thrown in LockIfNeeded.
             */
            for (int lockIndex = 0; lockIndex < config.MaxLocksPerOperation; lockIndex++)
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

        public bool TryGetValue(TKey key, out ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, false);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    value = default;
                    return false;
                }
                var valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                value = ReadFromDataFile(valueOffset);
                return true;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long WriteToDataFile(ReadOnlySpan<byte> value)
        {
            return config.DataFile.Write(value, dataBaseAddress);
        }

        private byte* FindRecordNonBlocking(ref NonBlockingOperationContext<TKey> context)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsNonBlockingOperationValid(ref NonBlockingOperationContext<TKey> context)
        {
            if (context.Versions == null) return true;
            int chunkIndex = (int)(context.InitialSlot >> config.ChunkBits);

            for (int i = 0; i <= context.VersionIndex; i++)
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

        public bool TryGetValueNonBlocking(TKey key, out ReadOnlySpan<byte> value)
        {

            int* versions = stackalloc int[config.MaxLocksPerOperation];
            var context = new NonBlockingOperationContext<TKey>
            {
                Versions = versions,
                InitialSlot = GetInitialSlot(key),
                Key = key
            };
            
            while (true)
            {
                long valueOffset;
                bool isFound;
                byte* recordPointer = FindRecordNonBlocking(ref context);
                if( recordPointer == null)
                {
                    isFound = true;
                    valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                }
                else
                {
                    isFound = false;
                    valueOffset = 0;
                }
                if (IsNonBlockingOperationValid(ref context))
                {
                    value = isFound ? ReadFromDataFile(valueOffset) : default ;
                    return isFound;
                }
            }
        }

        private void RobinHoodAdd(ref OperationContext<TKey> context, long valueOffset)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            short distance = 1; //start with 1 because 0 is reserved for empty slots.

            // set context to initial slot
            context.LockIndex = 0;
            context.RemainingSlotsInChunk = 0;
            context.CurrentSlot = context.InitialSlot;

            TKey key = context.Key;
            while (true)
            {
                LockIfNeeded(ref context);
                short currentRecordDistance = GetDistance(recordPointer);
                if (currentRecordDistance == 0) // found empty slot
                {
                    SetKey(recordPointer, key);
                    SetValueOffset(recordPointer, valueOffset);
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
                    long tempValueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));

                    SetKey(recordPointer, key);
                    SetValueOffset(recordPointer, valueOffset);
                    SetDistance(recordPointer, distance);
#if DEBUG
                    Interlocked.Add(ref config.HeaderPointer->DistanceSum, distance - currentRecordDistance);
#endif

                    key = tempKey;
                    valueOffset = tempValueOffset;
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

        public bool TryAdd(TKey key, ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    long valueOffset = WriteToDataFile(value);
                    RobinHoodAdd(ref context, valueOffset);
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

        public void Add(TKey key, ReadOnlySpan<byte> value)
        {
            if (!TryAdd(key, value))
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ReadOnlySpan<byte> ReadFromDataFile(long offset)
        {
            byte* pointer = dataBaseAddress + offset;
            return new ReadOnlySpan<byte>(pointer + sizeof(int), *(int*)pointer);
        }


        public ReadOnlySpan<byte> GetOrAdd(TKey key, ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                long valueOffset;
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    valueOffset = WriteToDataFile(value);
                    RobinHoodAdd(ref context, valueOffset);
                    return value;
                }
                valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                return ReadFromDataFile(valueOffset);
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public ReadOnlySpan<byte> GetOrAdd(TKey key, ValueFactory<TKey> valueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                long valueOffset;
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    var value = valueFactory(key);
                    valueOffset = WriteToDataFile(value);
                    RobinHoodAdd(ref context, valueOffset);
                    return value;
                }
                valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                return ReadFromDataFile(valueOffset);
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public ReadOnlySpan<byte> GetOrAdd<TArg>(TKey key, ValueFactory<TKey, TArg> valueFactory, TArg factoryArgument)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                long valueOffset;
                var recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    var value = valueFactory(key, factoryArgument);
                    valueOffset = WriteToDataFile(value);
                    RobinHoodAdd(ref context, valueOffset);
                    return value;
                }
                valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                return ReadFromDataFile(valueOffset);
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool TryRemove(TKey key, out ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
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
                        value = ReadFromDataFile(GetValueOffset(GetValueOffsetPointer(currentRecordPointer)));
                        Interlocked.Decrement(ref config.HeaderPointer->RecordCount);
                        return true;
                    }
                    SetKey(emptyRecordPointer, GetKey(currentRecordPointer));
                    SetValueOffset(emptyRecordPointer, GetValueOffset(currentRecordPointer));
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


        public void Put(TKey key, ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                long valueOffset = WriteToDataFile(value);
                if (recordPointer == null)
                {
                    RobinHoodAdd(ref context, valueOffset);
                }
                else
                {
                    SetValueOffset(recordPointer, valueOffset);
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool TryUpdate(TKey key, ReadOnlySpan<byte> newValue, ReadOnlySpan<byte> comparisonValue)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
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
                    long valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                    var value = ReadFromDataFile(valueOffset);
                    if (comparisonValue.SequenceEqual(value))
                    { 
                        long newValueOffset = WriteToDataFile(newValue);
                        SetValueOffset(recordPointer, newValueOffset);
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

        public ReadOnlySpan<byte> AddOrUpdate(TKey key, ReadOnlySpan<byte> addValue, UpdateValueFactory<TKey> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
               
                if (recordPointer == null)
                {
                    long valueOffset = WriteToDataFile(addValue);
                    RobinHoodAdd(ref context, valueOffset);
                    return addValue;
                }
                else
                {
                    long valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                    var value = ReadFromDataFile(valueOffset);
                    var newValue = updateValueFactory(key, value);
                    long newValueOffset = WriteToDataFile(newValue);
                    SetValueOffset(recordPointer, newValueOffset);
                    return newValue;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public ReadOnlySpan<byte> AddOrUpdate(TKey key, ValueFactory<TKey> addValueFactory, UpdateValueFactory<TKey> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);

                if (recordPointer == null)
                {
                    var addValue = addValueFactory(key);
                    long valueOffset = WriteToDataFile(addValue);
                    RobinHoodAdd(ref context, valueOffset);
                    return addValue;
                }
                else
                {
                    long valueOffset = GetValueOffset(GetValueOffsetPointer(recordPointer));
                    var value = ReadFromDataFile(valueOffset);
                    var newValue = updateValueFactory(key, value);
                    long newValueOffset = WriteToDataFile(newValue);
                    SetValueOffset(recordPointer, newValueOffset);
                    return newValue;
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }


        //public ICollection<TKey> Keys => new StaticFixedSizeHashTableKeyCollection<TKey, ReadOnlySpan<byte>>(this);

        //public ICollection<ReadOnlySpan<byte>> Values => new StaticFixedSizeHashTableValueCollection<TKey, ReadOnlySpan<byte>>(this);

        //int ICollection<KeyValuePair<TKey, ReadOnlySpan<byte>>>.Count => (int) Count;

        //public bool IsReadOnly => false;

        public ReadOnlySpan<byte> this[TKey key]
        {
            get
            {
                if (!TryGetValue(key, out ReadOnlySpan<byte> value))
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




        //public void Add(KeyValuePair<TKey, ReadOnlySpan<byte>> item)
        //{
        //    Add(item.Key, item.Value);
        //}

        //public void Clear()
        //{
        //    if (config.HeaderPointer->RecordCount > 0)
        //    {
        //        Memory.ZeroMemory(config.TablePointer, config.RecordSize * config.SlotCount);
        //        config.HeaderPointer->RecordCount = 0;
        //        config.HeaderPointer->MaxDistance = 0;
        //        config.HeaderPointer->DistanceSum = 0;
        //    }
        //}

        //public bool Contains(KeyValuePair<TKey, ReadOnlySpan<byte>> item)
        //{
        //    if (TryGetValue(item.Key, out ReadOnlySpan<byte> value))
        //    {
        //        return config.ValueComparer.Equals(item.Value, value);
        //    }
        //    return false;
        //}

        //public void CopyTo(KeyValuePair<TKey, ReadOnlySpan<byte>>[] array, int arrayIndex)
        //{
        //    if (array == null) throw new ArgumentNullException(nameof(array));
        //    if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "arrayIndex parameter must be greater than zero");
        //    if (this.Count > array.Length - arrayIndex) throw new ArgumentException("The array has not enough space to hold all items");
        //    foreach (var keyValue in this)
        //    {
        //        array[arrayIndex++] = keyValue;
        //    }
        //}

        //public bool Remove(KeyValuePair<TKey, ReadOnlySpan<byte>> item)
        //{
        //    return Remove(item.Key);
        //}


        //public IEnumerator<KeyValuePair<TKey, ReadOnlySpan<byte>>> GetEnumerator()
        //{
        //    return new StaticFixedSizeHashTableRecordEnumerator<TKey, ReadOnlySpan<byte>>(this);
        //}

        //IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }
}
