using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal class FixedSizeHashTableValueCollection<TKey, TValue> : ICollection<TValue> where TKey : unmanaged where TValue : unmanaged
    {

        private readonly FixedSizeHashTable<TKey, TValue> hashTable;

        public int Count => (int) hashTable.Count;

        public bool IsReadOnly => true;

        public FixedSizeHashTableValueCollection(FixedSizeHashTable<TKey, TValue> hashTable)
        {
            this.hashTable = hashTable;
        }

        public void Add(TValue item)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(TValue item)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(TValue[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "arrayIndex parameter must be greater than zero");
            if (this.Count > array.Length - arrayIndex) throw new ArgumentException("The array has not enough space to hold all items");
            
            foreach (var value in this)
            {
                array[arrayIndex++] = value;
            }
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            return new FixedSizeHashTableValueEnumerator<TKey, TValue>(hashTable);
        }

        public bool Remove(TValue item)
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
