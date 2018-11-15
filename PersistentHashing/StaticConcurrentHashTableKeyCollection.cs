/*
Copyright 2018 Jesús López Méndez

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal class StaticConcurrentHashTableKeyCollection<TKey, TValue, TK, TV> : ICollection<TKey> where TK : unmanaged where TV : unmanaged
    {

        private readonly StaticConcurrentAbstractHashTable<TKey, TValue, TK, TV> hashTable;

        public int Count => (int) hashTable.Count;

        public bool IsReadOnly => true;

        public StaticConcurrentHashTableKeyCollection(StaticConcurrentAbstractHashTable<TKey, TValue, TK, TV> hashTable)
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
            return new StaticConcurrentHashTableKeyEnumerator<TKey, TValue, TK, TV>(hashTable);
        }

        public bool Remove(TKey item)
        {
            return hashTable.Remove(item);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
