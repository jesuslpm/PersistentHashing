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
