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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public unsafe struct StaticHashTableConfig<TKey, TValue>
    {
        public long Capacity;
        public long SlotCount;
        public long HashMask;
        public long ChunkMask;
        public long ChunkSize;

        public string HashTableFilePath;
        public string DataFilePath;
        public SyncObject[] SyncObjects;
        public Func<TKey, long> HashFunction;
        public IEqualityComparer<TKey> KeyComparer;
        public IEqualityComparer<TValue> ValueComparer;
        public MemoryMapper TableMemoryMapper;
        public MemoryMappingSession TableMappingSession;
        public DataFile DataFile;
        public byte* TableFileBaseAddress;

        public StaticHashTableFileHeader* HeaderPointer;
        public byte* TablePointer;
        public byte* EndTablePointer;


        public int RecordSize;
        public int MaxLocksPerOperation;
        public int ChunkBits;
        public int ChunkCount;
        public int SlotBits;
        public int MaxAllowedDistance;

        public bool IsThreadSafe;
        public bool IsNew;
    }
}
