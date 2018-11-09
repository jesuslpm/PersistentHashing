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
    public unsafe sealed class StaticConcurrentFixedSizeHashTable<TKey, TValue>: IDisposable, IDictionary<TKey, TValue> where TKey:unmanaged where TValue:unmanaged
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



        internal StaticConcurrentFixedSizeHashTable(in StaticHashTableConfig<TKey, TValue> config)
        {
            this.config = config;
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

        public bool ContainsKey(TKey key)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
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
                value = GetValue(GetValuePointer(recordPointer));
                return true;
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsNonBlockingOperationValid(ref NonBlockingOperationContext<TKey> context)
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
            var context = new NonBlockingOperationContext<TKey>
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




        internal bool TryAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument, out TValue existingOrAddedValue)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
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
            var context = new OperationContext<TKey>();
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
            var context = new OperationContext<TKey>();
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


        public TValue AddOrUpdate<Targ>(TKey key, Func<TKey, Targ, TValue> addValueFactory, Targ addValueFactoryArg, Func<TKey, TValue, TValue> updateValueFactory)
        {
            long takenLocks = 0L;
            var context = new OperationContext<TKey>();
            InitializeOperationContext(ref context, key, &takenLocks, isWriting: true);
            try
            {
                byte* recordPointer = FindRecord(ref context);
                if (recordPointer == null)
                {
                    TValue value = addValueFactory(key, addValueFactoryArg);
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

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            // DRY version
            return AddOrUpdate(key, NestedValueFactory, addValueFactory, updateValueFactory);
        }


        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            // DRY version
            return AddOrUpdate(key, IdentityValueFactory, addValue, updateValueFactory);

            // WET version
            /*
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
            */
        }


        private void RobinHoodAdd(ref OperationContext<TKey> context,  TValue value)
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



        internal byte* FindRecord(ref OperationContext<TKey> context)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseLocks(ref OperationContext<TKey> context)
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

        public ICollection<TKey> Keys => new StaticConcurrentFixedSizeHashTableKeyCollection<TKey, TValue>(this);

        public ICollection<TValue> Values => new StaticConcurrentFixedSizeHashTableValueCollection<TKey, TValue>(this);

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
            return new StaticConcurrentFixedSizeHashTableRecordEnumerator<TKey, TValue>(this);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
