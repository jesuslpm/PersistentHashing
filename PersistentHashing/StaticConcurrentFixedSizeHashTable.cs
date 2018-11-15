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
