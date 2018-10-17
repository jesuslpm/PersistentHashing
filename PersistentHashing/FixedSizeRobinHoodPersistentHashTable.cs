using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace PersistentHashing
{
    public unsafe class FixedSizeRobinHoodPersistentHashTable<TKey, TValue>: IDisposable where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><slot-padding>

        uint keyOffset;
        uint valueOffset;
        uint distanceOffset;
        readonly bool isAligned;

        uint keySize;
        uint valueSize;
        uint distanceSize;
        uint recordSize;
        readonly ulong slotCount;
        readonly ulong mask;

        private MemoryMapper memoryMapper;
        private MemoryMappingSession mappingSession;
        private byte* fileBaseAddress;

        private FixedSizeRobinHoodHashTableFileHeader* headerPointer;
        private readonly byte* tablePointer;
        private readonly byte* endTablePointer;
        private readonly int bits;

        public int MaxDistance { get; private set; }

        private readonly Func<TKey, ulong> hashFunction;
        

        private const int AllocationGranularity = 64 * 1024;

        public FixedSizeRobinHoodPersistentHashTable(string filePath, long capacity, Func<TKey, ulong> hashFunction = null,  bool isAligned = false)
        {
            this.isAligned = isAligned;
            this.hashFunction = hashFunction;
            CalculateOffsetsAndSizes();
            slotCount = (ulong) Bits.NextPowerOf2(capacity);
            mask = (ulong) slotCount - 1UL;
            bits = Bits.MostSignificantBit(slotCount);

            var fileSize = (ulong) sizeof(FixedSizeRobinHoodHashTableFileHeader) +  slotCount * recordSize;
            fileSize += (AllocationGranularity - (fileSize & (AllocationGranularity - 1))) & (AllocationGranularity - 1);

            var isNew = !File.Exists(filePath);

            memoryMapper = new MemoryMapper(filePath, (long) fileSize);
            mappingSession = memoryMapper.OpenSession();

            fileBaseAddress = mappingSession.GetBaseAddress();
            headerPointer = (FixedSizeRobinHoodHashTableFileHeader*)fileBaseAddress;

            if (isNew)
            {
                WriteHeader();
            }
            else
            {
                ValidateHeader();
                slotCount = headerPointer->Slots;
            }
            tablePointer = fileBaseAddress + sizeof(FixedSizeRobinHoodHashTableFileHeader);
            endTablePointer = tablePointer + recordSize * slotCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(ulong slotIndex) => 
            tablePointer + recordSize * slotIndex;

        /// <summary>
        /// Returns the distance + 1 to the ideal slot index.
        /// The ideal slot index is the slot index where the hash maps to.
        /// distance == 0 means the slot is free. distance > 0 means the slot is occupied
        /// The distance is incremented by 1 to reserve zero value as free slot.
        /// </summary>
        /// <param name="recordPointer"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ushort GetDistance(byte* recordPointer) => 
            *(ushort*)(recordPointer + distanceOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetDistance(byte* recordPointer, ushort distance)
        {
            *(ushort*)(recordPointer + distanceOffset) = distance;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetKey(byte* recordPointer, TKey key)
        {
            *(TKey*)(recordPointer + keyOffset) = key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetValue(byte* recordPointer, TValue value)
        {
            *(TValue*)(recordPointer + valueOffset) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetKeyPointer(byte* recordPointer) => 
            recordPointer + keyOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetKeyAsSpan(byte* keyPointer) => 
            new ReadOnlySpan<byte>(keyPointer, (int)keySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static TKey GetKey(byte* keyPointer) => 
            *(TKey*)keyPointer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetValuePointer(byte* recordPointer) => 
            recordPointer + valueOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetValueAsSpan(byte *valuePointer) => 
            new ReadOnlySpan<byte>(valuePointer, (int)valueSize);

        static TValue GetValue(byte* valuePointer) => 
            *(TValue*)valuePointer;

        void WriteHeader()
        {
            headerPointer->IsAligned = isAligned;
            headerPointer->KeySize = keySize;
            headerPointer->Magic = FixedSizeRobinHoodHashTableFileHeader.MagicNumber;
            headerPointer->RecordCount = 0;
            headerPointer->RecordSize = recordSize;
            headerPointer->Reserved[0] = 0;
            headerPointer->Reserved[1] = 0;
            headerPointer->Reserved[2] = 0;
            headerPointer->Slots = slotCount;
            headerPointer->ValueSize = valueSize;
        }

        void ValidateHeader()
        {
            if ( headerPointer->Magic != FixedSizeRobinHoodHashTableFileHeader.MagicNumber)
            {
                throw new FormatException("This is not a FixedSizeRobinHoodHashTableFile");
            }
            if (headerPointer->IsAligned != isAligned)
            {
                throw new ArgumentException("Mismatched alignement");
            }
            if (headerPointer->KeySize != keySize)
            {
                throw new ArgumentException("Mismatched keySize");
            }
            if (headerPointer->ValueSize != valueSize)
            {
                throw new ArgumentException("Mismatched ValueSize");
            }
            if (headerPointer->RecordSize != recordSize)
            {
                throw new ArgumentException("Mismatched SlotSize");
            }
        }

        private void CalculateOffsetsAndSizes()
        {
            keySize = (uint) sizeof(TKey);
            valueSize = (uint) sizeof(TValue);
            distanceSize = sizeof(ushort);


            uint keyAlignement = GetAlignement(keySize);
            uint valueAlignement = GetAlignement(valueSize);
            uint distanceAlignement = GetAlignement(distanceSize);
            uint slotAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            keyOffset = 0;
            valueOffset = keyOffset + keySize + GetPadding(keyOffset + keySize, valueAlignement);
            distanceOffset = valueOffset + valueSize + GetPadding(valueOffset + valueSize, distanceAlignement);
            recordSize = distanceOffset + distanceSize + GetPadding(distanceOffset + distanceSize, slotAlignement);
        }

        private uint GetPadding(uint offsetWithoutPadding, uint alignement)
        {
            if (!isAligned) return 0u;
            uint alignementMinusOne = alignement - 1u;
            // (alignement - offsetWithoutPadding % alignement) % alignement
            return (alignement - (offsetWithoutPadding & alignementMinusOne)) & alignementMinusOne;
        }

        private uint GetAlignement(uint size)
        {
            if (!isAligned) return 1u;
            if (size >= 8u) return 8u;
            if (size >= 4u) return 4u;
            if (size >= 2u) return 2u;
            return 1u;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ulong GetIdealSlotIndex(TKey key)
        {
            if (hashFunction == null)
            {
                return Hashing.FastHash64((byte*)&key, keySize) & mask;
            }
            else
            {
                return hashFunction(key) * 11400714819323198485LU & mask;
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            var idealSlotIndex = GetIdealSlotIndex(key);
            var recordPointer = FindRecordPointer(idealSlotIndex, key);
            if (recordPointer == null)
            {
                value = default;
                return false;
            }
            value = GetValue(GetValuePointer(recordPointer));
            return true;
        }

        public void Add(TKey key, TValue value)
        {
            var idealSlotIndex = GetIdealSlotIndex(key);
            var recordPointer = FindRecordPointer(idealSlotIndex, key);
            if (recordPointer == null)
            {
                RobinHoodAdd(idealSlotIndex, key, value);
            }
            else
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }

        public void Put(TKey key, TValue value)
        {
            var idealSlotIndex = GetIdealSlotIndex(key);
            var recordPointer = FindRecordPointer(idealSlotIndex, key);
            if (recordPointer == null)
            {
                RobinHoodAdd(idealSlotIndex, key, value);
            }
            else
            {
                *(TValue*)GetValuePointer(recordPointer) = value;
                //Unsafe.AsRef<TValue>(GetValuePointer(recordPointer)) = value;
            }
        }

        private void RobinHoodAdd(ulong idealSlotIndex, TKey key, TValue value)
        {
            byte* recordPointer = GetRecordPointer(idealSlotIndex);
            ushort distance = 1; //start with 1 because 0 is reserved for free slots.
            while (true)
            {
                ushort currentRecordDistance = GetDistance(recordPointer);
                if (currentRecordDistance == 0)
                {
                    SetKey(recordPointer, key);
                    SetValue(recordPointer, value);
                    SetDistance(recordPointer, distance);
                    if (MaxDistance < distance) MaxDistance = distance;
                    return;
                }
                else if (currentRecordDistance < distance)
                {
                    /* Swap Robin Hood style */
                    TKey tempKey = GetKey(GetKeyPointer(recordPointer));
                    TValue tempValue = GetValue(GetValuePointer(recordPointer));
                    SetKey(recordPointer, key);
                    SetValue(recordPointer, value);
                    SetDistance(recordPointer, distance);
                    key = tempKey;
                    value = tempValue;
                    distance = currentRecordDistance;
                }
                distance++;
                if (distance > 10_000)
                {
                    throw new InvalidOperationException("Reached max distance");
                }
                recordPointer += recordSize;
                if (recordPointer >= endTablePointer)
                {
                    recordPointer = tablePointer;
                }
            }
        }

        private byte* FindRecordPointer(ulong idealSlotIndex, TKey key)
        {
            byte* recordPointer = GetRecordPointer(idealSlotIndex);
            byte* keyPointer = (byte*) &key;
            while (true)
            {
                if (GetDistance(recordPointer) == 0) return null;
                if (Memory.Compare(keyPointer, GetKeyPointer(recordPointer), (int) keySize) == 0)
                {
                    return recordPointer;
                }
                recordPointer += recordSize;
                if (recordPointer >= endTablePointer)
                {
                    recordPointer = tablePointer;
                }
            }
        }

        public bool IsDisposed { get; private set; }

        public void Dispose()
        {
            if (IsDisposed) return;
            IsDisposed = true;
            if (this.mappingSession != null) this.mappingSession.Dispose();
            if (this.memoryMapper != null) this.memoryMapper.Dispose();
        }

        public void Flush()
        {
            this.memoryMapper.Flush();
        }
    }
}
