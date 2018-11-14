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
    /// <summary>
    /// Represents a fixed capacity, thread safe, persistent hash table.
    /// </summary>
    /// <typeparam name="TKey">The key type</typeparam>
    /// <typeparam name="TValue">The value type</typeparam>
    /// <typeparam name="TK">The type of table record field K</typeparam>
    /// <typeparam name="TV">The type of table record field V</typeparam>
    public unsafe abstract class AbstractStaticConcurrentHashTable<TKey, TValue, TK, TV>: IDisposable 
        where TK:unmanaged where TV:unmanaged
    {

        // <k><v><distance>

        protected internal StaticHashTableConfig<TKey, TValue> config;

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



        public AbstractStaticConcurrentHashTable(in StaticHashTableConfig<TKey, TValue> config)
        {
            this.config = config;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(long slot) => config.TablePointer + config.RecordSize * slot;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //ref StaticHashTableRecord<TK, TV> Record(long slot)
        //{
        //    return ref Unsafe.AsRef<StaticHashTableRecord<TK, TV>>(GetRecordPointer(slot));
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ref StaticHashTableRecord<TK, TV> Record(void* recordPointer)
        {
            return ref Unsafe.AsRef<StaticHashTableRecord<TK, TV>>(recordPointer);
        }

        protected abstract TKey GetKey(in StaticHashTableRecord<TK, TV> record);
        protected abstract TValue GetValue(in StaticHashTableRecord<TK, TV> record);
        protected abstract StaticHashTableRecord<TK, TV> StoreItem(TKey key, TValue value, long hash);
        protected abstract bool AreKeysEqual(in StaticHashTableRecord<TK, TV> record, TKey key, long hash);


        /// <summary>
        /// Returns the intial slot index corresponding to the key. 
        /// It is the index that the key hash maps to.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetInitialSlot(TKey key)
        {
            return config.HashFunction(key) & config.HashMask;
        }


        public bool ContainsKey(TKey key)
        {
            long takenLocks = 0L;
            var context = new OperationContext(config, key, &takenLocks, false);
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
            var context = new OperationContext(config, key, &takenLocks, false);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    value = default;
                    return false;
                }
                value = GetValue(Record(recordPointer));
                return true;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsNonBlockingOperationValid(in NonBlockingOperationContext context)
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
            // stackalloc memory is initialized to 0
            // https://stackoverflow.com/questions/8679052/initialization-of-memory-allocated-with-stackalloc/53173980#53173980
            int* versions = stackalloc int[config.MaxLocksPerOperation];

            var context = new NonBlockingOperationContext(config, versions, key);
            while (true)
            {
                byte* recordPointer = FindRecordNonBlocking(ref context);
                bool isFound = recordPointer != null;
                value = recordPointer ==  null ? default : GetValue(Record(recordPointer));
                if (IsNonBlockingOperationValid(context)) return isFound;
            }
        }

        public void Add(TKey key, TValue value)
        {
            if (!TryAdd(key, value))
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }




        internal bool TryAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument, out TValue existingOrAddedValue)
        {
            long takenLocks = 0L;
            var context = new OperationContext(config, key, &takenLocks, true); 
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null) // not found
                {
                    existingOrAddedValue = valueFactory(key, factoryArgument); // added value
                    RobinHoodAdd(ref context, existingOrAddedValue);
                    return true;
                }
                existingOrAddedValue = GetValue(Record(recordPointer)); // existing value
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
            var context = new OperationContext(config, key, &takenLocks, true);
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
            var context = new OperationContext(config, key, &takenLocks, true);
            try
            {
                byte* emptyRecordPointer = FindRecord(ref context);
                if (emptyRecordPointer == null)
                {
                    value = default;
                    return false;
                }
                byte* currentRecordPointer = emptyRecordPointer;
                ref var emptyRecord = ref Record(emptyRecordPointer);
#if DEBUG
                Interlocked.Add(ref config.HeaderPointer->DistanceSum, 1 - emptyRecord.Distance);
#endif

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
                    ref var currentRecord = ref Record(currentRecordPointer);
                    if (currentRecord.Distance <= 1) // empty record or zero distance
                    { 
                        emptyRecord.Distance = 0;
                        value = GetValue(currentRecord);
                        Interlocked.Decrement(ref config.HeaderPointer->RecordCount);
                        return true;
                    }
                    emptyRecord = currentRecord;
                    emptyRecord.Distance--;
#if DEBUG
                    Interlocked.Decrement(ref config.HeaderPointer->DistanceSum);
#endif
                    emptyRecord = ref currentRecord;
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
            return TryRemove(key, out var removedValue);
        }

        public void Put(TKey key, TValue value)
        {
            long takenLocks = 0L;
            var context = new OperationContext(config, key, &takenLocks, true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    RobinHoodAdd(ref context, value);
                }
                else
                {
                    Update(key, value, recordPointer, context.Hash);
                }
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Update(TKey key, TValue value, byte* recordPointer, long hash)
        {
            var newRecordValue = StoreItem(key, value, hash);
            ref var record = ref Record(recordPointer);
            record.KeyOrHash = newRecordValue.KeyOrHash;
            record.ValueOrOffset = newRecordValue.ValueOrOffset;
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            long takenLocks = 0L;
            var context = new OperationContext(config, key, &takenLocks, true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    return false;
                }
                else
                {
                    var value = GetValue(Record(recordPointer));
                    if (config.ValueComparer.Equals(value, comparisonValue))
                    {
                        Update(key, newValue, recordPointer, context.Hash);
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


        public TValue AddOrUpdate<Targ>(TKey key, Func<TKey, Targ, TValue> addValueFactory, Targ addValueFactoryArg, Func<TKey, TValue, TValue> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext(config, key, &takenLocks, true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null) // not found, add it
                {
                    TValue value = addValueFactory(key, addValueFactoryArg);
                    RobinHoodAdd(ref context, value);
                    return value;
                }
                else // found, update it
                {
                    TValue value = GetValue(Record(recordPointer));
                    TValue newValue = updateValueFactory(key, value);
                    Update(key, newValue, recordPointer, context.Hash);
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
            // DRY version
            return AddOrUpdate(key, NestedValueFactory, addValueFactory, updateValueFactory);
        }


        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            // DRY version
            return AddOrUpdate(key, IdentityValueFactory, addValue, updateValueFactory);
        }


        private void RobinHoodAdd(ref OperationContext context, TValue value)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            short distance = 1; //start with 1 because 0 is reserved for empty slots.

            // set context to initial slot
            context.LockIndex = 0;
            context.RemainingSlotsInChunk = 0;
            context.CurrentSlot = context.InitialSlot;
            TKey key = context.Key;
            var newRecord = StoreItem(key, value, context.Hash);
            
            while (true)
            {
                LockIfNeeded(ref context);
                ref var currentRecord = ref Record(recordPointer);
                short currentRecordDistance = currentRecord.Distance;
                if (currentRecordDistance == 0) // found empty slot
                {
                    currentRecord = newRecord;
                    currentRecord.Distance = distance;
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
                    var tempRecord = currentRecord;
                    currentRecord = newRecord;
                    currentRecord.Distance = distance;

#if DEBUG
                    Interlocked.Add(ref config.HeaderPointer->DistanceSum, distance - currentRecordDistance);
#endif
                    newRecord = tempRecord;
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
                ref var record = ref Record(recordPointer);
                if (record.Distance == 0) return null;
                if (AreKeysEqual(record, context.Key, context.Hash)) return recordPointer;
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



        internal byte* FindRecord(ref OperationContext context)
        {
            byte* recordPointer = GetRecordPointer(context.InitialSlot);
            int distance = 1;

            while (true)
            {
                ref var record = ref Record(recordPointer);
                LockIfNeeded(ref context);

                if (record.Distance == 0) return null;
                if (AreKeysEqual(Record(recordPointer), context.Key, context.Hash))
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

        //public ICollection<TKey> Keys => new StaticConcurrentFixedSizeHashTableKeyCollection<TKey, TValue>(this);

        //public ICollection<TValue> Values => new StaticConcurrentFixedSizeHashTableValueCollection<TKey, TValue>(this);

        //int ICollection<KeyValuePair<TKey, TValue>>.Count => (int) Count;

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

        //public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        //{
        //    if (array == null) throw new ArgumentNullException(nameof(array));
        //    if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "arrayIndex parameter must be greater than zero");
        //    if (this.Count > array.Length - arrayIndex) throw new ArgumentException("The array has not enough space to hold all items");
        //    foreach (var keyValue in this)
        //    {
        //        array[arrayIndex++] = keyValue;
        //    }
        //}

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        //public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        //{
        //    return new StaticConcurrentFixedSizeHashTableRecordEnumerator<TKey, TValue>(this);
        //}

        //IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        internal unsafe struct OperationContext
        {
            public long InitialSlot;
            public long RemainingSlotsInChunk;
            public long CurrentSlot;
            public long Hash;
            public TKey Key;
            public int LockIndex;
            public bool* TakenLocks;
            public bool IsWriting;


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public OperationContext(in StaticHashTableConfig<TKey, TValue> config, TKey key, void* takenLocks, bool isWriting)
            {
                Hash = config.HashFunction(key);
                CurrentSlot = InitialSlot = Hash & config.HashMask;
                Key = key;
                TakenLocks = (bool*)takenLocks;
                IsWriting = isWriting;
                RemainingSlotsInChunk = 0;
                LockIndex = 0;
            }
        }

        internal unsafe struct NonBlockingOperationContext
        {
            public long InitialSlot;
            public long Hash;
            public int* Versions;
            public int VersionIndex;
            public TKey Key;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public NonBlockingOperationContext(in StaticHashTableConfig<TKey, TValue> config, int* versions, TKey key)
            {
                Hash = config.HashFunction(key);
                InitialSlot = Hash & config.HashMask;
                Key = key;
                Versions = versions;
                VersionIndex = 0;
            }
        }
    }
}
