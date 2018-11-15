using System;
using System.Runtime.CompilerServices;

namespace PersistentHashing
{
    public unsafe sealed class StaticVariableSizeStore: StaticStore<MemorySlice, MemorySlice> 
    {

        private long initialDataFileSize;
        private int dataFileSizeGrowthIncrement;

        public StaticVariableSizeStore(string filePathPathWithoutExtension, long capacity,  
           VariableSizeHashTableOptions options = null)
            : base(filePathPathWithoutExtension, capacity, new BaseHashTableOptions<MemorySlice, MemorySlice>
            {
                HashFunction = options?.HashFunction ?? Hashing.FastHash64,
                KeyComparer = MemorySlice.EqualityComparer,
                ValueComparer = MemorySlice.EqualityComparer
            })
        {
            initialDataFileSize = options?.InitialDataFileSize ?? 8 * 1024 * 1024;
            dataFileSizeGrowthIncrement = options?.DataFileSizeGrowthIncrement ?? 4 * 1024 * 1024; 
        }

        protected override int GetRecordSize()
        {
            return Unsafe.SizeOf<StaticHashTableRecord<long, long>>();
        }

        public StaticConcurrentVariableSizeHashTable GetConcurrentHashTable()
        {
            config.IsThreadSafe = true;
            EnsureInitialized();
            return new StaticConcurrentVariableSizeHashTable(config);
        }


        public StaticConcurrentVariableSizeHashTable GetThreadUnsafeHashTable()
        {
            config.IsThreadSafe = true;
            EnsureInitialized();
            return new StaticConcurrentVariableSizeHashTable(config);
        }

        protected override DataFile OpenDataFile()
        {
            return new DataFile(config.DataFilePath, initialDataFileSize, dataFileSizeGrowthIncrement);
        }
    }
}
