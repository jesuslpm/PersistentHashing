using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal unsafe class FixedSizeHashTableRecordEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly FixedSizeHashTable<TKey, TValue> hashTable;

        public FixedSizeHashTableRecordEnumerator(FixedSizeHashTable<TKey, TValue> hashTable )
        {
            this.hashTable = hashTable;
            recordPointer = hashTable.tablePointer;
        }

        public KeyValuePair<TKey, TValue> Current
        {
            get
            {
                if (recordPointer >= hashTable.tablePointer)
                {
                    throw new InvalidOperationException("No more records");
                }
                return new KeyValuePair<TKey, TValue>(
                    FixedSizeHashTable<TKey, TValue>.GetKey(hashTable.GetKeyPointer(recordPointer)), 
                    FixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer)));
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            if (recordPointer < hashTable.endTablePointer)
            {
                recordPointer += hashTable.recordSize;
                return true;
            }
            return false;
        }

        public void Reset()
        {
            recordPointer = hashTable.tablePointer;
        }
    }

}
