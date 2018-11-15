using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal class StaticConcurrentHashTableValueCollection<TKey, TValue, TK, TV> : ICollection<TValue> where TK : unmanaged where TV : unmanaged
    {

        private readonly StaticConcurrentAbstractHashTable<TKey, TValue, TK, TV> hashTable;

        public int Count => (int)hashTable.Count;

        public bool IsReadOnly => true;

        public StaticConcurrentHashTableValueCollection(StaticConcurrentAbstractHashTable<TKey, TValue, TK, TV> hashTable)
        {
            this.hashTable = hashTable;
        }

        public void Add(TValue value)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public bool Contains(TValue value)
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
            return new StaticConcurrentHashTableValueEnumerator<TKey, TValue, TK, TV>(hashTable);
        }

        public bool Remove(TValue value)
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }
}
