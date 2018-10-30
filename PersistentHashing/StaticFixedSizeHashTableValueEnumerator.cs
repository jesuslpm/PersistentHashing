﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    internal unsafe class StaticFixedSizeHashTableValueEnumerator<TKey, TValue> : IEnumerator<TValue> where TKey:unmanaged where TValue : unmanaged
    {
        private byte* recordPointer;
        private readonly StaticFixedSizeHashTable<TKey, TValue> hashTable;
   
        public StaticFixedSizeHashTableValueEnumerator(StaticFixedSizeHashTable<TKey, TValue> hashTable )
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
                return StaticFixedSizeHashTable<TKey, TValue>.GetValue(hashTable.GetValuePointer(recordPointer));
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
