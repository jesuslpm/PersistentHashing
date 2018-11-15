using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public class StaticConcurrentFixedSizeHashTable<TKey, TValue>
        : StaticConcurrentAbstractHashTable<TKey, TValue, TKey, TValue>
        where TKey : unmanaged where TValue : unmanaged
    {

        private readonly StaticFixedSizeStore<TKey, TValue> store;

        internal StaticConcurrentFixedSizeHashTable(in StaticHashTableConfig<TKey, TValue> config, StaticFixedSizeStore<TKey, TValue> store)
            :base(config)
        {
            this.store = store;
        }

        protected override bool AreKeysEqual(in StaticHashTableRecord<TKey, TValue> record, TKey key, long hash)
        {
            return config.KeyComparer.Equals(record.KeyOrHash, key);
        }

        protected internal override TKey GetKey(in StaticHashTableRecord<TKey, TValue> record)
        {
            return record.KeyOrHash;
        }

        protected internal override TValue GetValue(in StaticHashTableRecord<TKey, TValue> record)
        {
            return record.ValueOrOffset;
        }

        protected internal override StaticHashTableRecord<TKey, TValue> StoreItem(TKey key, TValue value, long hash)
        {
            return new StaticHashTableRecord<TKey, TValue>(key, value);
        }

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed) return;
            base.Dispose(disposing);
            if (disposing)
            {
                if (store != null) store.Dispose();
            }
        }
    }
}
