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

    public unsafe class StaticConcurrentFixedKeySizeHashTable<TKey>
        : StaticConcurrentAbstractHashTable<TKey, MemorySlice, TKey, long>
        where TKey : unmanaged
    {
        internal MemoryMappingSession mappingSession;

        internal byte* dataPointer;

        public StaticConcurrentFixedKeySizeHashTable(in StaticHashTableConfig<TKey, MemorySlice> config)
            :base(config)
        {
            mappingSession = config.DataFile.OpenSession();
            mappingSession.BaseAddressChanged += MappingSession_BaseAddressChanged;
            dataPointer = mappingSession.GetBaseAddress();
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

        protected internal override MemorySlice GetValue(in StaticHashTableRecord<TKey, long> record)
        {
            return new MemorySlice(dataPointer + record.ValueOrOffset + sizeof(int), *(int *)(dataPointer + record.ValueOrOffset));
        }

        protected internal override StaticHashTableRecord<TKey, long> StoreItem(TKey key, MemorySlice value, long hash)
        {
            long valueOffset = config.DataFile.Allocate(value.Size + sizeof(int));
            *(int*)(dataPointer + valueOffset) = value.Size;
            var destinationSpan = new Span<byte>(dataPointer + valueOffset + sizeof(int), value.Size);
            value.ToReadOnlySpan().CopyTo(destinationSpan);
            return new StaticHashTableRecord<TKey, long>(key, valueOffset);
        }

        public bool TryAdd(TKey key, byte[] value)
        {
            fixed (byte* pointer = value)
            {
                return TryAdd(key, new MemorySlice(pointer, value.Length));
            }
        }


        public bool TryAdd<TItem>(TKey key, TItem[] value) where TItem: unmanaged
        {
            fixed (TItem* pointer = value)
            {
                return TryAdd(key, new MemorySlice(pointer, value.Length * sizeof(TItem)));
            }
        }

        public bool TryAdd<TItem>(TKey key, ReadOnlySpan<TItem> value) where TItem : unmanaged
        {
            fixed (TItem* pointer = value)
            {
                return TryAdd(key, new MemorySlice(pointer, value.Length * sizeof(TItem)));
            }
        }

        public void Add<TItem>(TKey key, TItem[] value) where TItem: unmanaged
        {
            fixed (TItem* pointer = value)
            {
                Add(key, new MemorySlice(pointer, value.Length * sizeof(TItem)));
            }
        }

        public void Add<TItem>(TKey key, ReadOnlySpan<TItem> value) where TItem : unmanaged
        {
            fixed (TItem* pointer = value)
            {
                Add(key, new MemorySlice(pointer, value.Length * sizeof(TItem)));
            }
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
