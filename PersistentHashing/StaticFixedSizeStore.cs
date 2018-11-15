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
    internal unsafe sealed class StaticFixedSizeStore<TKey, TValue>: StaticStore<TKey, TValue>, IDisposable 
        where TKey:unmanaged 
        where TValue: unmanaged
    {


        internal StaticFixedSizeStore(string filePathPathWithoutExtension, long capacity, Func<TKey, long> hashFunction, HashTableComparers<TKey, TValue> comparers = null)
            :base(filePathPathWithoutExtension, capacity, 
                 new BaseHashTableOptions<TKey, TValue> { HashFunction = hashFunction, KeyComparer = comparers.KeyComparer, ValueComparer = comparers.ValueComparer })
        {
        }


        protected override int GetRecordSize()
        {
            return Unsafe.SizeOf<StaticHashTableRecord<TKey, TValue>>();
        }

        public StaticConcurrentFixedSizeHashTable<TKey, TValue> GetConcurrentHashTable()
        {
            config.IsThreadSafe = true;
            EnsureInitialized();
            return new StaticConcurrentFixedSizeHashTable<TKey, TValue>(config, this);
        }

        protected override DataFile OpenDataFile()
        {
            return null;
        }
    }
}
