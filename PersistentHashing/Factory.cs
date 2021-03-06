﻿/*
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

    public static class Factory
    {
        public static StaticConcurrentFixedSizeHashTable<TKey, TValue> GetStaticConcurrentFixedSizeHashTable<TKey, TValue>(string filePathWithoutExtension, long capacity, Func<TKey, long> hashFunction,  HashTableComparers<TKey, TValue> comparers = null)
            where TKey : unmanaged where TValue : unmanaged
        {
            var store = new StaticFixedSizeStore<TKey, TValue>(filePathWithoutExtension, capacity, hashFunction, comparers) ;
            return store.GetConcurrentHashTable();
        }

        public static StaticFixedKeySizeStore<TKey> GetStaticFixedKeySizeStore<TKey>(string filePathWithoutExtension, long capacity, Func<TKey, long> hashFunction, HashTableOptions<TKey, MemorySlice> options = null) 
            where TKey: unmanaged
        {
            return new StaticFixedKeySizeStore<TKey>(filePathWithoutExtension, capacity, hashFunction, options);
        }

        public static StaticFixedKeySizeStore<TKey, TValue> GetStaticFixedKeySizeStore<TKey, TValue>(string filePathWithoutExtension, long capacity, Func<TKey, long> hashFunction, IValueSerializer<TValue> valueSerializer, HashTableOptions<TKey, TValue> options = null)
            where TKey : unmanaged
        {
            return new StaticFixedKeySizeStore<TKey, TValue>(filePathWithoutExtension, capacity, hashFunction, valueSerializer, options);
        }

        public static StaticStore GetStaticStore(string filePathWithoutExtension, long capacity, Func<MemorySlice, long> hashFunction = null, HashTableOptions<MemorySlice, MemorySlice> options = null)
        {
            return new StaticStore(filePathWithoutExtension, capacity, hashFunction, options);
        }

        public static StaticStore<TKey, TValue> GetStaticStore<TKey, TValue>(string filePathWithoutExtension, long capacity, Func<TKey, long> hashFunction, ItemSerializer<TKey, TValue> itemSerializer, HashTableOptions<TKey, TValue> options = null)
        {
            return new StaticStore<TKey, TValue>(filePathWithoutExtension, capacity, hashFunction, itemSerializer, options);
        }


        public static object GetStaticConcurrentStore()
        {
            throw new NotImplementedException();
        }

        public static object GetDynamicStore()
        {
            throw new NotImplementedException();
        }

        public static object GetDynamicConcurrentStore()
        {
            throw new NotImplementedException();
        }
    }
}
