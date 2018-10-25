using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    internal unsafe class StaticFixedSizeHashTableRecordEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly StaticFixedSizeHashTable<TKey, TValue> hashTable;
        private long slot;
        private KeyValuePair<TKey, TValue> _current;
        

        public StaticFixedSizeHashTableRecordEnumerator(StaticFixedSizeHashTable<TKey, TValue> hashTable )
        {
            this.hashTable = hashTable;
            recordPointer = hashTable.tablePointer - hashTable.recordSize;
            slot = -1;
        }

        public KeyValuePair<TKey, TValue> Current
        {
            get
            {
                if (slot < 0 || slot >= hashTable.slotCount)
                {
                    throw new InvalidOperationException("No current record");
                }
                return _current;
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
                slot++;
                if (hashTable.ThreadSafety == ThreadSafety.Safe)
                {
                    bool lockTaken = false;
#if SPINLATCH
                    SpinLatch.Enter(ref hashTable.syncObjects[slot >> hashTable.chunkBits], ref lockTaken);
#else
                    Monitor.Enter(hashTable.syncObjects[slot >> hashTable.chunkBits], ref lockTaken);
#endif
                    try
                    {
                        if (hashTable.GetDistance(recordPointer) > 0)
                        {
                            _current = new KeyValuePair<TKey, TValue>(
                                StaticFixedSizeHashTable<TKey, TValue>.GetKey(hashTable.GetKeyPointer(recordPointer)),
                                StaticFixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer)));
                            return true;
                        }
                    }
                    finally
                    {
#if SPINLATCH
                        SpinLatch.Exit(ref hashTable.syncObjects[slot >> hashTable.chunkBits]);
#else
                        Monitor.Exit(hashTable.syncObjects[slot >> hashTable.chunkBits]);
#endif
                    }
                }
                else if (hashTable.GetDistance(recordPointer) > 0)
                { 
                    _current = new KeyValuePair<TKey, TValue>(
                        StaticFixedSizeHashTable<TKey, TValue>.GetKey(hashTable.GetKeyPointer(recordPointer)),
                        StaticFixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer)));
                    return true;
                }
            }
            return false;
        }

        public void Reset()
        {
            recordPointer = hashTable.tablePointer - hashTable.recordSize;
            slot = -1;
        }
    }
}
