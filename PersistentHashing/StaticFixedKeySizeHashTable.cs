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


    public unsafe sealed class StaticFixedKeySizeHashTable<TKey>: IDisposable where TKey:unmanaged
    {
        // <key><value-offset-padding><value-offset><distance padding><distance16><record-padding>

        private readonly StaticFixedSizeHashTable<TKey, long> hashTable;

        internal StaticFixedKeySizeHashTable(ref StaticHashTableConfig<TKey, long> config)
        {
            hashTable = new StaticFixedSizeHashTable<TKey, long>(ref config);
        }


        //public bool TryGetValue(TKey key, out ReadOnlySpan<byte> value)
        //{
            
        //}


        //public bool TryGetValueNonBlocking(TKey key, out ReadOnlySpan<byte> value)
        //{
        //}

        //public void Add(TKey key, ReadOnlySpan<byte> value)
        //{
        //    if (!TryAdd(key, value))
        //    {
        //        throw new ArgumentException($"An element with the same key {key} already exists");
        //    }
        //}

        //public bool TryAdd(TKey key, ReadOnlySpan<byte> value)
        //{

        //}

        //public ReadOnlySpan<byte> GetOrAdd(TKey key, ReadOnlySpan<byte> value)
        //{
        //}

        //public ReadOnlySpan<byte> GetOrAdd(TKey key, ValueFactory<TKey> valueFactory)
        //{

        //}

        //public ReadOnlySpan<byte> GetOrAdd<TArg>(TKey key, ValueFactory<TKey, TArg> valueFactory, TArg factoryArgument)
        //{
        //}

        //public bool TryRemove(TKey key, out ReadOnlySpan<byte> value)
        //{

        //}


        //public void Put(TKey key, ReadOnlySpan<byte> value)
        //{

        //}

        public bool TryUpdate(TKey key, ReadOnlySpan<byte> newValue, ReadOnlySpan<byte> comparisonValue)
        {
            return true;
        }

        //public ReadOnlySpan<byte> AddOrUpdate(TKey key, ReadOnlySpan<byte> addValue, Func<TKey, ReadOnlySpan<byte>, ReadOnlySpan<byte>> updateValueFactory)
        //{

        //}

        //public ReadOnlySpan<byte> AddOrUpdate(TKey key, Func<TKey, ReadOnlySpan<byte>> addValueFactory, Func<TKey, ReadOnlySpan<byte>, ReadOnlySpan<byte>> updateValueFactory)
        //{

        //}

        ////[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //private void RobinHoodAdd(ref OperationContext context,  ReadOnlySpan<byte> value)
        //{
 
        //}



        //public void Delete(TKey key)
        //{
        //    if (!Remove(key))
        //    {
        //        throw new ArgumentException($"key {key} not found");
        //    }
        //}

        //public bool Remove(TKey key)
        //{

        //}


        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //private byte* FindRecordNonBlocking(ref NonBlockingOperationContext context)
        //{

        //}






        public bool IsDisposed { get; private set; }

        //public ICollection<TKey> Keys => new StaticFixedSizeHashTableKeyCollection<TKey, ReadOnlySpan<byte>>(this);

        //public ICollection<ReadOnlySpan<byte>> Values => new StaticFixedSizeHashTableValueCollection<TKey, ReadOnlySpan<byte>>(this);

        //int ICollection<KeyValuePair<TKey, ReadOnlySpan<byte>>>.Count => (int) Count;

        //public bool IsReadOnly => false;

        //public ReadOnlySpan<byte> this[TKey key]
        //{
        //    get
        //    {
        //        if (!TryGetValue(key, out ReadOnlySpan<byte> value))
        //        {
        //            throw new ArgumentException($"Key {key} not found.");
        //        }
        //        return value;
        //    }
        //    set
        //    {
        //        Put(key, value);
        //    }
        //}

        public void Dispose()
        {
            if (IsDisposed) return;
            IsDisposed = true;
            //if (this.config.TableMappingSession != null) this.config.TableMappingSession.Dispose();
            //if (this.config.TableMemoryMapper != null) this.config.TableMemoryMapper.Dispose();
        }

        //public void Flush()
        //{
        //    this.config.TableMemoryMapper.Flush();
        //}

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
