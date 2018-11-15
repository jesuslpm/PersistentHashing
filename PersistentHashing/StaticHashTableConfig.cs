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
