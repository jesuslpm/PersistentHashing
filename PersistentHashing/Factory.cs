using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{

    public static class Factory
    {
        //public static StaticFixedSizeHashTable<TKey, TValue> GetStaticFixedSizeHashTable<TKey, TValue>(string filePathWithoutExtension, long capacity, HashTableOptions<TKey, TValue> options = null)  
        //    where TKey:unmanaged 
        //    where TValue:unmanaged
        //{
        //    throw new NotImplementedException();
        //}

        public static object GetStaticFixedKeySizeStore<TKey>(string filePathWithoutExtension)
            where TKey : unmanaged
        {
            throw new NotImplementedException();
        }

        public static object GetStaticVariableSizeStore(string filePathWithoutExtension)
        {
            throw new NotImplementedException();
        }

        public static object GetStaticConcurrentFixedSizeHashTable(string filePathWithoutExtension)
        {
            throw new NotImplementedException();
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
