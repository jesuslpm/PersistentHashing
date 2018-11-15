using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    internal unsafe class StaticConcurrentHashTableKeyEnumerator<TKey, TValue, TK, TV> : IEnumerator<TKey> where TK:unmanaged where TV : unmanaged
    {
        private byte* recordPointer;
        private readonly StaticConcurrentAbstractHashTable<TKey, TValue, TK, TV> hashTable;
        private long slot;
        private TKey _current;

        public StaticConcurrentHashTableKeyEnumerator(StaticConcurrentAbstractHashTable<TKey, TValue, TK, TV> hashTable)
        {
            this.hashTable = hashTable;
            recordPointer = hashTable.config.TablePointer - hashTable.config.RecordSize;
            slot = -1;
        }

        public TKey Current
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
                    ref var record = ref hashTable.Record(recordPointer);
                    if (record.Distance > 0)
                    {
                        _current = hashTable.GetKey(record);
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
            recordPointer = hashTable.config.TablePointer;
        }
    }
}
