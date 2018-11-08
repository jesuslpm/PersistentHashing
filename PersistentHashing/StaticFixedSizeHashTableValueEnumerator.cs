using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal unsafe class StaticConcurrentFixedSizeHashTableValueEnumerator<TKey, TValue> : IEnumerator<TValue> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly StaticConcurrentFixedSizeHashTable<TKey, TValue> hashTable;
   
        public StaticConcurrentFixedSizeHashTableValueEnumerator(StaticConcurrentFixedSizeHashTable<TKey, TValue> hashTable )
        {
            this.hashTable = hashTable;
            recordPointer = hashTable.config.TablePointer;
        }

        public TValue Current
        {
            get
            {
                if (recordPointer >= hashTable.config.TablePointer)
                {
                    throw new InvalidOperationException("No more records");
                }
                return StaticConcurrentFixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer));
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }


        public bool MoveNext()
        {
            while (recordPointer < hashTable.config.EndTablePointer)
            {
                recordPointer += hashTable.config.RecordSize;
                if (hashTable.GetDistance(recordPointer) > 0) return true;
            }
            return false;
        }

        public void Reset()
        {
            recordPointer = hashTable.config.TablePointer;
        }
    }
}
