using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal class StaticConcurrentFixedSizeHashTableKeyCollection<TKey, TValue> : ICollection<TKey> where TKey : unmanaged where TValue : unmanaged
    {

        private readonly StaticConcurrentFixedSizeHashTable<TKey, TValue> hashTable;

        public int Count => (int) hashTable.Count;

        public bool IsReadOnly => true;

        public StaticConcurrentFixedSizeHashTableKeyCollection(StaticConcurrentFixedSizeHashTable<TKey, TValue> hashTable)
        {
            this.hashTable = hashTable;
        }

        public void Add(TKey item)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(TKey item)
        {
            return hashTable.ContainsKey(item);
        }

        public void CopyTo(TKey[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "arrayIndex parameter must be greater than zero");
            if (this.Count > array.Length - arrayIndex) throw new ArgumentException("The array has not enough space to hold all items");
            
            foreach (var key in this)
            {
                array[arrayIndex++] = key;
            }
        }

        public IEnumerator<TKey> GetEnumerator()
        {
            return new StaticConcurrentFixedSizeHashTableKeyEnumerator<TKey, TValue>(hashTable);
        }

        public bool Remove(TKey item)
        {
            return hashTable.Remove(item);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
