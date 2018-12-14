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
    public unsafe class DynamicHashTableSizeState
    {
        public long Capacity;
        /// <summary>
        /// The number of slots in the table
        /// </summary>
        public long SlotCount;
        /// <summary>
        /// The number of slots in the table plus the number of slots in the overflow area
        /// </summary>
        public long TotalSlotCount;

        /// <summary>
        /// The number of slots in the overflow area
        /// </summary>
        public int OverflowAreaSlotCount;

        public long HashMask;
        public int SlotBits;

        public byte* TablePointer;
        public byte* EndTablePointer;
        public byte* TableFileBaseAddress;
        public StaticHashTableFileHeader* HeaderPointer;

        public const long ChunkMask = 0x1F;
        public const int ChunkSize = 32;
        public const int ChunkBits = 5;
    }


    public unsafe struct DynamicHashTableConfig<TKey, TValue>
    {
        public string HashTableFilePath;
        public string DataFilePath;
        public Func<TKey, long> HashFunction;
        public IEqualityComparer<TKey> KeyComparer;
        public IEqualityComparer<TValue> ValueComparer;
        public MemoryMapper TableMemoryMapper;
        public MemoryMappingSession TableMappingSession;
        public DataFile DataFile;
        public int RecordSize;

        public bool IsThreadSafe;
        public bool IsNew;
    }
}
