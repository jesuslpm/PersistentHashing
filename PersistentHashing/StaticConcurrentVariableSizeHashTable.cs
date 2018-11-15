using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{

    public unsafe class StaticConcurrentVariableSizeHashTable
        : StaticConcurrentAbstractHashTable<MemorySlice, MemorySlice, long, long>
    {
        internal MemoryMappingSession mappingSession;

        internal byte* dataPointer;

        public StaticConcurrentVariableSizeHashTable(in StaticHashTableConfig<MemorySlice, MemorySlice> config)
            :base(config)
        {
            mappingSession = config.DataFile.OpenSession();
            mappingSession.BaseAddressChanged += MappingSession_BaseAddressChanged;
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

            long keyValueOffset = config.DataFile.Allocate(key.Size + value.Size + 2 * sizeof(int));
            var keyPointer = dataPointer + keyValueOffset;
            *(int*)keyPointer = key.Size;
            var destinationKeySpan = new Span<byte>(keyPointer + sizeof(int), key.Size);
            key.ToReadOnlySpan().CopyTo(destinationKeySpan);

            var valuePointer = keyPointer + sizeof(int) + key.Size;
            *(int*)valuePointer = value.Size;
            var destinationValueSpan = new Span<byte>(valuePointer + sizeof(int), value.Size);
            value.ToReadOnlySpan().CopyTo(destinationValueSpan);

            return new StaticHashTableRecord<long, long>(hash, keyValueOffset);
        }
    }
}
