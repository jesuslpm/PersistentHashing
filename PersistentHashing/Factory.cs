using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{

    public static class Factory
    {
        public static StaticFixedSizeHashTable<TKey, TValue> GetStaticFixedSizeHashTable<TKey, TValue>(string filePathWithoutExtension, long capacity, HashTableOptions<TKey, TValue> options = null)  
            where TKey:unmanaged 
            where TValue:unmanaged
        {
            throw new NotImplementedException();
        }

        public static object GetStaticFixedKeySizeStore<TKey>(string filePathWithoutExtension)
            where TKey : unmanaged
        {
            throw new NotImplementedException();
        }

        public static object GetStaticVariableSizeStore(string filePathWithoutExtension)
        {
            throw new NotImplementedException();
        }

        public static object GetConcurrentStaticFixedSizeHashTable(string filePathWithoutExtension)
        {
            throw new NotImplementedException();
        }

        public static object GetConcurrentStaticFixedKeySizeStore(string filePathWithoutExtension)
        {
            throw new NotImplementedException();
        }

        public static object GetConcurrentStaticVariableSizeStore()
        {
            throw new NotImplementedException();
        }

        public static object GetDynamicVariableSizeStore()
        {
            throw new NotImplementedException();
        }

        public static object GetConcurrentDynamicVariableSizeStore()
        {
            throw new NotImplementedException();
        }
    }
}
