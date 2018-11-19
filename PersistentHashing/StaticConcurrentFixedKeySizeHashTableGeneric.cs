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

    public unsafe class StaticConcurrentFixedKeySizeHashTable<TKey, TValue>
        : StaticConcurrentAbstractHashTable<TKey, TValue, TKey, long>
        where TKey : unmanaged
    {
        internal MemoryMappingSession mappingSession;

        internal byte* dataPointer;

        private readonly IValueSerializer<TValue> valueSerializer;

        public StaticConcurrentFixedKeySizeHashTable(in StaticHashTableConfig<TKey, TValue> config, IValueSerializer<TValue> valueSerializer)
            :base(config)
        {
            mappingSession = config.DataFile.OpenSession();
            mappingSession.BaseAddressChanged += MappingSession_BaseAddressChanged;
            dataPointer = mappingSession.GetBaseAddress();
            this.valueSerializer = valueSerializer;
        }

        private void MappingSession_BaseAddressChanged(object sender, MemoryMappingSession.BaseAddressChangedEventArgs e)
        {
            dataPointer = e.BaseAddress;
        }

        protected override bool AreKeysEqual(in StaticHashTableRecord<TKey, long> record, TKey key, long hash)
        {
            return config.KeyComparer.Equals(record.KeyOrHash, key); ;
        }

        protected internal override TKey GetKey(in StaticHashTableRecord<TKey, long> record)
        {
            return record.KeyOrHash;
        }

        protected internal override TValue GetValue(in StaticHashTableRecord<TKey, long> record)
        {
            return valueSerializer.Deserialize(dataPointer + record.ValueOrOffset);
        }

        protected internal override StaticHashTableRecord<TKey, long> StoreItem(TKey key, TValue value, long hash)
        {
            var target = new SerializationTarget(config.DataFile, dataPointer);
            valueSerializer.Serialize(value, target);
            if (target.dataOffset == 0) throw new InvalidOperationException("SerializationTarget.GetTargetSlice must be called");
            return new StaticHashTableRecord<TKey, long>(key, target.dataOffset);
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
