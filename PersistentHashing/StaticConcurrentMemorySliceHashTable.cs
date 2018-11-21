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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{

    public unsafe class StaticConcurrentHashTable
        : StaticConcurrentAbstractHashTable<MemorySlice, MemorySlice, long, long>
    {
        internal MemoryMappingSession mappingSession;

        internal byte* dataPointer;

        public StaticConcurrentHashTable(in StaticHashTableConfig<MemorySlice, MemorySlice> config)
            :base(config)
        {
            mappingSession = config.DataFile.OpenSession();
            mappingSession.BaseAddressChanged += MappingSession_BaseAddressChanged;
            dataPointer = mappingSession.GetBaseAddress();
            if (this.config.KeyComparer == null) this.config.KeyComparer = MemorySlice.EqualityComparer;
            if (this.config.HashFunction == null) this.config.HashFunction = Hashing.FastHash64;
        }

        private void MappingSession_BaseAddressChanged(object sender, MemoryMappingSession.BaseAddressChangedEventArgs e)
        {
            dataPointer = e.BaseAddress;
        }

        protected override bool AreKeysEqual(in StaticHashTableRecord<long, long> record, MemorySlice key, long hash)
        {
            // having the Hash in the table record is an important optimization
            // because we only need to go to data file to get the key if the hashes are equal.
            // We have 64 bit hashes, therefore if hashes are equal it's is very likely that keys are also equal.
            // Going to datafile is expensive because data is in a different page than the record
            // outside the processor cache, and can cause a hard page fault.
            if (record.KeyOrHash == hash)
            {
                return config.KeyComparer.Equals(GetKey(record), key);
            }
            return false;
        }

        protected internal override MemorySlice GetKey(in StaticHashTableRecord<long, long> record)
        {
            return new MemorySlice(dataPointer + record.ValueOrOffset + sizeof(int), *(int*)(dataPointer + record.ValueOrOffset));
        }

        protected internal override MemorySlice GetValue(in StaticHashTableRecord<long, long> record)
        {
            var keySize = *(int*)(dataPointer + record.ValueOrOffset);
            var valuePointer = dataPointer + record.ValueOrOffset + sizeof(int) + keySize;
            return new MemorySlice(valuePointer + sizeof(int), *(int *)valuePointer);
        }

        protected internal override StaticHashTableRecord<long, long> StoreItem(MemorySlice key, MemorySlice value, long hash)
        {
            // we store key and value contiguously in the data file.

            var item = config.DataFile.AllocateItem(key.Size, value.Size);
            key.ToReadOnlySpan().CopyTo(item.KeySpan);
            value.ToReadOnlySpan().CopyTo(item.ValueSpan);
            return new StaticHashTableRecord<long, long>(hash, item.Offset);
        }

        public bool TryAdd<TKeyItem, TValueItem>(TKeyItem[] key, TValueItem[] value) where TKeyItem: unmanaged where TValueItem: unmanaged
        {
            fixed (void* keyPointer = key)
            fixed (void* valuePointer = value)
            {
                return TryAdd(new MemorySlice(keyPointer, key.Length * sizeof(TKeyItem)), new MemorySlice(valuePointer, value.Length * sizeof(TValueItem)));
            }
        }

        public void Add<TKeyItem, TValueItem>(TKeyItem[] key, TValueItem[] value) where TKeyItem : unmanaged where TValueItem : unmanaged
        {
            fixed (void* keyPointer = key)
            fixed (void* valuePointer = value)
            {
                Add(new MemorySlice(keyPointer, key.Length * sizeof(TKeyItem)), new MemorySlice(valuePointer, value.Length * sizeof(TValueItem)));
            }
        }

        public MemorySlice Get<TKeyItem>(TKeyItem[] key) where TKeyItem: unmanaged
        {
            fixed (void* keyPointer = key)
            {
                return this[new MemorySlice(keyPointer, key.Length * sizeof(TKeyItem))];
            }
        }

        public bool ContainsKey<TKeyItem>(TKeyItem[] key) where TKeyItem: unmanaged
        {
            fixed (void* keyPointer = key)
            {
                return ContainsKey(new MemorySlice(keyPointer, key.Length * sizeof(TKeyItem)));
            }
        }

        public bool Remove<TKeyItem>(TKeyItem[] key) where TKeyItem: unmanaged
        {
            fixed (void* keyPointer = key)
            {
                return Remove(new MemorySlice(keyPointer, key.Length * sizeof(TKeyItem)));
            }
        }

        public bool TryRemove<TKeyItem>(TKeyItem[] key, out MemorySlice value) where TKeyItem : unmanaged
        {
            fixed (void* keyPointer = key)
            {
                return TryRemove(new MemorySlice(keyPointer, key.Length * sizeof(TKeyItem)), out value);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed) return;
            base.Dispose(disposing);
            if (disposing && mappingSession != null) mappingSession.Dispose();
        }
    }
}
