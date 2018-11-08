using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    internal unsafe class StaticConcurrentFixedSizeHashTableRecordEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly StaticConcurrentFixedSizeHashTable<TKey, TValue> hashTable;
        private long slot;
        private KeyValuePair<TKey, TValue> _current;
        

        public StaticConcurrentFixedSizeHashTableRecordEnumerator(StaticConcurrentFixedSizeHashTable<TKey, TValue> hashTable )
        {
            this.hashTable = hashTable;
            recordPointer = hashTable.config.TablePointer - hashTable.config.RecordSize;
            slot = -1;
        }

        public KeyValuePair<TKey, TValue> Current
        {
            get
            {
                if (slot < 0 || slot >= hashTable.config.SlotCount)
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
            while (recordPointer < hashTable.config.EndTablePointer)
            {
                recordPointer += hashTable.config.RecordSize;
                slot++;
                bool lockTaken = false;
#if SPINLATCH
                SpinLatch.Enter(ref hashTable.syncObjects[slot >> hashTable.chunkBits], ref lockTaken);
#else
                Monitor.Enter(hashTable.config.SyncObjects[slot >> hashTable.config.ChunkBits], ref lockTaken);
#endif
                try
                {
                    if (hashTable.GetDistance(recordPointer) > 0)
                    {
                        _current = new KeyValuePair<TKey, TValue>(
                            StaticConcurrentFixedSizeHashTable<TKey, TValue>.GetKey(hashTable.GetKeyPointer(recordPointer)),
                            StaticConcurrentFixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer)));
                        return true;
                    }
                }
                finally
                {
#if SPINLATCH
                    SpinLatch.Exit(ref hashTable.syncObjects[slot >> hashTable.chunkBits]);
#else
                    Monitor.Exit(hashTable.config.SyncObjects[slot >> hashTable.config.ChunkBits]);
#endif
                }

            }
            return false;
        }

        public void Reset()
        {
            recordPointer = hashTable.config.TablePointer - hashTable.config.RecordSize;
            slot = -1;
        }
    }
}
