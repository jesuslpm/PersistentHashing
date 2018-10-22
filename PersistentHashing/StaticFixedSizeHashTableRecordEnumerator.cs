using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal unsafe class StaticFixedSizeHashTableRecordEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly StaticFixedSizeHashTable<TKey, TValue> hashTable;

        public StaticFixedSizeHashTableRecordEnumerator(StaticFixedSizeHashTable<TKey, TValue> hashTable )
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
                    StaticFixedSizeHashTable<TKey, TValue>.GetKey(hashTable.GetKeyPointer(recordPointer)), 
                    StaticFixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer)));
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
