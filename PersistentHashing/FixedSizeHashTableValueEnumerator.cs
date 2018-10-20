using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal unsafe class FixedSizeHashTableValueEnumerator<TKey, TValue> : IEnumerator<TValue> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly FixedSizeHashTable<TKey, TValue> hashTable;
   
        public FixedSizeHashTableValueEnumerator(FixedSizeHashTable<TKey, TValue> hashTable )
        {
            this.hashTable = hashTable;
            recordPointer = hashTable.tablePointer;
        }

        public TValue Current
        {
            get
            {
                if (recordPointer >= hashTable.tablePointer)
                {
                    throw new InvalidOperationException("No more records");
                }
                return FixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer));
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }


        public bool MoveNext()
        {
            while (recordPointer < hashTable.endTablePointer)
            {
                recordPointer += hashTable.recordSize;
                if (hashTable.GetDistance(recordPointer) > 0) return true;
            }
            return false;
        }

        public void Reset()
        {
            recordPointer = hashTable.tablePointer;
        }
    }
}
