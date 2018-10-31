using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    internal unsafe struct StaticHashTableConfig<TKey, TValue>
    {
        public long Capacity;
        public long SlotCount;
        public long HashMask;
        public long ChunkMask;
        public long ChunkSize;

        public SyncObject[] SyncObjects;
        public Func<TKey, long> HashFunction;
        public IEqualityComparer<TKey> KeyComparer;
        public IEqualityComparer<TValue> ValueComparer;
        public MemoryMapper TableMemoryMapper;
        public MemoryMappingSession TableMappingSession;
        public byte* TableFileBaseAddress;
        public ThreadSafety ThreadSafety;

        public StaticFixedSizeHashTableFileHeader* HeaderPointer;
        public byte* TablePointer;
        public byte* EndTablePointer;

        public int KeyOffset;
        public int ValueOffset;
        public int DistanceOffset;
        public int HashOffset;
        public int KeySize;
        public int ValueSize;
        public int DistanceSize;
        public int RecordSize;
        public int MaxLocksPerOperation;
        public int ChunkBits;
        public int MaxAllowedDistance;

        public bool IsAligned;
    }
}
