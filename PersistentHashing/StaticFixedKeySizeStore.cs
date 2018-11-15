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
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public unsafe sealed class StaticFixedKeySizeStore<TKey>: StaticStore<TKey, MemorySlice> where TKey: unmanaged
    {

        private long initialDataFileSize;
        private int dataFileSizeGrowthIncrement;

        public StaticFixedKeySizeStore(string filePathPathWithoutExtension, long capacity, Func<TKey, long> hashFunction,
            FixedKeySizeHashTableOptions<TKey> options)
            : base(filePathPathWithoutExtension, capacity, 
                  new BaseHashTableOptions<TKey, MemorySlice> { HashFunction = hashFunction, KeyComparer = options.KeyComparer, ValueComparer = MemorySlice.EqualityComparer })
        {
            initialDataFileSize = options?.InitialDataFileSize ?? 8 * 1024 * 1024;
            dataFileSizeGrowthIncrement = options?.DataFileSizeGrowthIncrement ?? 4 * 1024 * 1024;
        }


        protected override int GetRecordSize()
        {
            return Unsafe.SizeOf<StaticHashTableRecord<TKey, long>>();
        }

        public StaticConcurrentFixedKeySizeHashTable<TKey> GetConcurrentHashTable()
        {
            config.IsThreadSafe = true;
            EnsureInitialized();
            return new StaticConcurrentFixedKeySizeHashTable<TKey>(config);
        }

        public StaticConcurrentFixedKeySizeHashTable<TKey> GetThreadUnsafeHashTable()
        {
            EnsureInitialized();
            return new StaticConcurrentFixedKeySizeHashTable<TKey>(config);
        }

        protected override DataFile OpenDataFile()
        {
            return new DataFile(config.DataFilePath, initialDataFileSize, dataFileSizeGrowthIncrement);
        }
    }
}
