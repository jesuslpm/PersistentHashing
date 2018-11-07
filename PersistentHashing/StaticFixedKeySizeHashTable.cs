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



    public unsafe sealed class StaticFixedKeySizeHashTable<TKey>: StaticFixedSizeHashTable<TKey, long> where TKey:unmanaged
    {
        // <key><value-offset-padding><value-offset><distance padding><distance16><record-padding>

        private readonly StaticFixedSizeHashTable<TKey, long> hashTable;
        private readonly MemoryMappingSession dataSession;


        internal StaticFixedKeySizeHashTable(ref StaticHashTableConfig<TKey, long> config): base(ref config)
        {
        }



        public bool TryGetValue(TKey key, out ReadOnlySpan<byte> value)
        {
            if (hashTable.TryGetValue(key, out long valueOffset))
            {
                value = ReadFromDataFile(valueOffset);
                return true;
            }
            value = default;
            return false;
        }


        public bool TryGetValueNonBlocking(TKey key, out ReadOnlySpan<byte> value)
        {
            if (hashTable.TryGetValueNonBlocking(key, out long valueOffset))
            {
                value = ReadFromDataFile(valueOffset);
                return true;
            }
            value = default;
            return false;
        }

        public bool TryAdd(TKey key, ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
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
            var context = new OperationContext();
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
                valueOffset = GetValue(GetValuePointer(recordPointer));
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
            var context = new OperationContext();
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
                valueOffset = GetValue(GetValuePointer(recordPointer));
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
            var context = new OperationContext();
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
                valueOffset = GetValue(GetValuePointer(recordPointer));
                return ReadFromDataFile(valueOffset);
            }
            finally
            {
                ReleaseLocks(ref context);
            }
        }

        public bool TryRemove(TKey key, out ReadOnlySpan<byte> value)
        {
            if (TryRemove(key, out long valueOffset))
            {
                value = ReadFromDataFile(valueOffset);
                return true;
            }
            value = default;
            return false;
        }


        public void Put(TKey key, ReadOnlySpan<byte> value)
        {
            long takenLocks = 0L;
            var context = new OperationContext();
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
                    SetValue(recordPointer, valueOffset);
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
                    long valueOffset = GetValue(GetValuePointer(recordPointer));
                    var value = ReadFromDataFile(valueOffset);
                    if (comparisonValue.SequenceEqual(value))
                    { 
                        long newValueOffset = WriteToDataFile(newValue);
                        SetValue(recordPointer, newValueOffset);
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
            var context = new OperationContext();
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
                    long valueOffset = GetValue(GetValuePointer(recordPointer));
                    var value = ReadFromDataFile(valueOffset);
                    var newValue = updateValueFactory(key, value);
                    long newValueOffset = WriteToDataFile(newValue);
                    SetValue(recordPointer, newValueOffset);
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
            var context = new OperationContext();
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
                    long valueOffset = GetValue(GetValuePointer(recordPointer));
                    var value = ReadFromDataFile(valueOffset);
                    var newValue = updateValueFactory(key, value);
                    long newValueOffset = WriteToDataFile(newValue);
                    SetValue(recordPointer, newValueOffset);
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

        public new ReadOnlySpan<byte> this[TKey key]
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
