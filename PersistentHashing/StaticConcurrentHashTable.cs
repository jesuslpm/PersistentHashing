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
        where TKey : unmanaged
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
            return itemSerializer.DeserializeKey(dataPointer + record.ValueOrOffset);
        }

        protected internal override TValue GetValue(in StaticHashTableRecord<long, long> record)
        {
            return itemSerializer.DeserializeValue(dataPointer + record.ValueOrOffset);
        }

        protected internal override StaticHashTableRecord<long, long> StoreItem(TKey key, TValue value, long hash)
        {
            var target = new SerializationTarget(config.DataFile, dataPointer);
            itemSerializer.Serialize(key, value, target);

            if (target.dataOffset == 0) throw new InvalidOperationException("SerializationTarget.GetTargetAddress must be called");
            return new StaticHashTableRecord<long, long>(hash, target.dataOffset);
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
