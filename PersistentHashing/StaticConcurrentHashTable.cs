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


namespace PersistentHashing
{

    public unsafe class StaticConcurrentHashTable<TKey, TValue>
        : StaticConcurrentAbstractHashTable<TKey, TValue, long, long>
    {
        internal MemoryMappingSession mappingSession;

        internal byte* dataPointer;

        private readonly IValueSerializer<TValue> valueSerializer;
        private ItemSerializer<TKey, TValue> itemSerializer;

        public StaticConcurrentHashTable(in StaticHashTableConfig<TKey, TValue> config, ItemSerializer<TKey, TValue> itemSerializer)
            :base(config)
        {
            mappingSession = config.DataFile.OpenSession();
            mappingSession.BaseAddressChanged += MappingSession_BaseAddressChanged;
            dataPointer = mappingSession.GetBaseAddress();
            this.itemSerializer = itemSerializer;
        }

        private void MappingSession_BaseAddressChanged(object sender, MemoryMappingSession.BaseAddressChangedEventArgs e)
        {
            dataPointer = e.BaseAddress;
        }


        protected override bool AreKeysEqual(in StaticHashTableRecord<long, long> record, TKey key, long hash)
        {
            if (record.KeyOrHash == hash)
            {
                return config.KeyComparer.Equals(GetKey(record), key);
            }
            return false;
        }

        protected internal override TKey GetKey(in StaticHashTableRecord<long, long> record)
        {
            byte* keyAddress = dataPointer + record.ValueOrOffset;
            return itemSerializer.DeserializeKey(new ReadOnlySpan<byte>(keyAddress + sizeof(int), *(int*)keyAddress));
        }

        protected internal override TValue GetValue(in StaticHashTableRecord<long, long> record)
        {
            byte* keyAddress = dataPointer + record.ValueOrOffset;
            byte* valueAddress = keyAddress + sizeof(int) + *(int*)keyAddress;
            return itemSerializer.DeserializeValue(new ReadOnlySpan<byte>(valueAddress + sizeof(int), *(int*)valueAddress));
        }

        protected internal override StaticHashTableRecord<long, long> StoreItem(TKey key, TValue value, long hash)
        {
            var offset = itemSerializer.Serialize(key, value, config.DataFile);
            return new StaticHashTableRecord<long, long>(hash, offset);
        }

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed) return;
            base.Dispose(disposing);
            if (disposing)
            {
                if (mappingSession != null) mappingSession.Dispose();
            }
        }
    }
}
