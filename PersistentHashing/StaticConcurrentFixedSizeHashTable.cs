using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public class StaticConcurrentFixedSizeHashTable<TKey, TValue>
        : AbstractStaticConcurrentHashTable<TKey, TValue, TKey, TValue>
        where TKey : unmanaged where TValue : unmanaged
    {

        public StaticConcurrentFixedSizeHashTable(in StaticHashTableConfig<TKey, TValue> config)
            :base(config)
        {
        }

        protected override bool AreKeysEqual(in StaticHashTableRecord<TKey, TValue> record, TKey key, long hash)
        {
            return config.KeyComparer.Equals(record.KeyOrHash, key);
        }

        protected override TKey GetKey(in StaticHashTableRecord<TKey, TValue> record)
        {
            return record.KeyOrHash;
        }

        protected override TValue GetValue(in StaticHashTableRecord<TKey, TValue> record)
        {
            return record.ValueOrOffset;
        }

        protected override StaticHashTableRecord<TKey, TValue> StoreItem(TKey key, TValue value, long hash)
        {
            return new StaticHashTableRecord<TKey, TValue>(key, value);
        }
    }
}
