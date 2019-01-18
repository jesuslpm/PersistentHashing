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
            var end = hashTable.config.SlotCount - 1;
            while (slot < end)
            {
                recordPointer += hashTable.config.RecordSize;
                slot++;
                bool lockTaken = false;
#if SPINLATCH
                SpinLatch.Enter(ref hashTable.config.SyncObjects[slot >> hashTable.config.ChunkBits].Locked, ref lockTaken);
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
                    SpinLatch.Exit(ref hashTable.config.SyncObjects[slot >> hashTable.config.ChunkBits].Locked);
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
