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

        public static StaticFixedKeySizeStore<TKey> GetStaticFixedKeySizeStore<TKey>(string filePathWithoutExtension, long capacity, Func<TKey, long> hashFunction, FixedKeySizeHashTableOptions<TKey> options = null) 
            where TKey: unmanaged
        {
            return new StaticFixedKeySizeStore<TKey>(filePathWithoutExtension, capacity, hashFunction, options);
        }

        public static StaticVariableSizeStore GetStaticVariableSizeStore(string filePathWithoutExtension, long capacity, VariableSizeHashTableOptions options = null)
        {
            return new StaticVariableSizeStore(filePathWithoutExtension, capacity, options);
        }


        public static object GetStaticConcurrentFixedKeySizeStore(string filePathWithoutExtension)
        {
            throw new NotImplementedException();
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
