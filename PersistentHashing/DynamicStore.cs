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
    public unsafe sealed class DynamicStore<TKey, TValue> : AbstractDynamicStore<TKey, TValue>
    {

        private long initialDataFileSize;
        private int dataFileSizeGrowthIncrement;
        private readonly ItemSerializer<TKey, TValue> itemSerializer;

        public DynamicStore(string filePathPathWithoutExtension, long capacity, Func<TKey, long> hashFunction,
            ItemSerializer<TKey, TValue> itemSerializer, HashTableOptions<TKey, TValue> options = null)
            : base(filePathPathWithoutExtension, capacity, new BaseHashTableOptions<TKey, TValue>
            {
                HashFunction = hashFunction,
                KeyComparer = options?.KeyComparer,
                ValueComparer = options?.ValueComparer
            })
        {
            initialDataFileSize = options?.InitialDataFileSize ?? 8 * 1024 * 1024;
            dataFileSizeGrowthIncrement = options?.DataFileSizeGrowthIncrement ?? 4 * 1024 * 1024;
            this.itemSerializer = itemSerializer;
        }

        protected override int GetRecordSize()
        {
            return Unsafe.SizeOf<StaticHashTableRecord<long, long>>();
        }

        //public StaticConcurrentHashTable<TKey, TValue> GetConcurrentHashTable()
        //{
        //    config.IsThreadSafe = true;
        //    EnsureInitialized();
        //    return new StaticConcurrentHashTable<TKey, TValue>(config, itemSerializer);
        //}


        //public StaticConcurrentHashTable<TKey, TValue> GetThreadUnsafeHashTable()
        //{
        //    config.IsThreadSafe = true;
        //    EnsureInitialized();
        //    return new StaticConcurrentHashTable<TKey, TValue>(config, itemSerializer);
        //}

        protected override DataFile OpenDataFile()
        {
            return new DataFile(config.DataFilePath, initialDataFileSize, dataFileSizeGrowthIncrement);
        }
    }

    public unsafe sealed class DynamicStore: AbstractDynamicStore<MemorySlice, MemorySlice> 
    {

        private long initialDataFileSize;
        private int dataFileSizeGrowthIncrement;

        public DynamicStore(string filePathPathWithoutExtension, long capacity, Func<MemorySlice, long> hashFunction = null,
           HashTableOptions<MemorySlice, MemorySlice> options = null)
            : base(filePathPathWithoutExtension, capacity, new BaseHashTableOptions<MemorySlice, MemorySlice>
            {
                HashFunction = hashFunction ?? Hashing.FastHash64,
                KeyComparer = options?.KeyComparer ?? MemorySlice.EqualityComparer,
                ValueComparer = options?.ValueComparer ?? MemorySlice.EqualityComparer
            })
        {
            initialDataFileSize = options?.InitialDataFileSize ?? 8 * 1024 * 1024;
            dataFileSizeGrowthIncrement = options?.DataFileSizeGrowthIncrement ?? 4 * 1024 * 1024; 
        }

        protected override int GetRecordSize()
        {
            return Unsafe.SizeOf<StaticHashTableRecord<long, long>>();
        }

        //public StaticConcurrentHashTable GetConcurrentHashTable()
        //{
        //    config.IsThreadSafe = true;
        //    EnsureInitialized();
        //    return new StaticConcurrentHashTable(config);
        //}


        //public StaticConcurrentHashTable GetThreadUnsafeHashTable()
        //{
        //    config.IsThreadSafe = true;
        //    EnsureInitialized();
        //    return new StaticConcurrentHashTable(config);
        //}

        protected override DataFile OpenDataFile()
        {
            return new DataFile(config.DataFilePath, initialDataFileSize, dataFileSizeGrowthIncrement);
        }
    }
}
