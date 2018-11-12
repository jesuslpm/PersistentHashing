using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{

    public unsafe class StaticConcurrentFixedKeySizeHashTable<TKey>
        : AbstractStaticConcurrentHashTable<TKey, MemorySlice, TKey, long>
        where TKey : unmanaged
    {
        internal MemoryMappingSession mappingSession;

        internal byte* dataPointer;

        public StaticConcurrentFixedKeySizeHashTable(in StaticHashTableConfig<TKey, MemorySlice> config)
            :base(config)
        {
            mappingSession = config.DataFile.OpenSession();
            mappingSession.BaseAddressChanged += MappingSession_BaseAddressChanged;
        }

        private void MappingSession_BaseAddressChanged(object sender, MemoryMappingSession.BaseAddressChangedEventArgs e)
        {
            dataPointer = e.BaseAddress;
        }

        protected override bool AreKeysEqual(in StaticHashTableRecord<TKey, long> record, TKey key, long hash)
        {
            return config.KeyComparer.Equals(record.KeyOrHash, key); ;
        }

        protected override TKey GetKey(in StaticHashTableRecord<TKey, long> record)
        {
            return record.KeyOrHash;
        }

        protected override MemorySlice GetValue(in StaticHashTableRecord<TKey, long> record)
        {
            return new MemorySlice(dataPointer + record.ValueOrOffset + sizeof(int), *(int *)(dataPointer + record.ValueOrOffset));
        }

        protected override StaticHashTableRecord<TKey, long> StoreItem(TKey key, MemorySlice value, long hash)
        {
            long valueOffset = config.DataFile.Allocate(value.Size + sizeof(int));
            *(int*)(dataPointer + valueOffset) = value.Size;
            var destinationSpan = new Span<byte>(dataPointer + valueOffset + sizeof(int), value.Size);
            value.ToReadOnlySpan().CopyTo(destinationSpan);
            return new StaticHashTableRecord<TKey, long>(key, valueOffset);
        }
    }
}
