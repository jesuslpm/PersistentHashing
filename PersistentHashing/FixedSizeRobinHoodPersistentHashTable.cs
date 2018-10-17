using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace PersistentHashing
{
    public unsafe class FixedSizeRobinHoodPersistentHashTable<TKey, TValue>: IDisposable where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><record-padding>

        uint keyOffset;
        uint valueOffset;
        uint distanceOffset;

        bool isAligned;

        uint keySize;
        uint valueSize;
        uint distanceSize;
        uint recordSize;
        ulong slots;
        ulong mask;

        private MemoryMapper memoryMapper;
        private MemoryMappingSession mappingSession;
        private byte* fileBaseAddress;

        private FixedSizeRobinHoodHashTableFileHeader* headerPointer;
        private byte* tablePointer;
        private byte* endTablePointer;


        private const int AllocationGranularity = 64 * 1024;

        public FixedSizeRobinHoodPersistentHashTable(string filePath, long capacity, bool isAligned = false)
        {
            this.isAligned = isAligned;
            CalculateOffsetsAndSizes();
            slots = (ulong) Bits.NextPowerOf2(capacity);
            mask = (ulong) slots - 1UL;

            var fileSize = (ulong) sizeof(FixedSizeRobinHoodHashTableFileHeader) +  slots * recordSize;
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
                slots = headerPointer->Slots;
            }
            tablePointer = fileBaseAddress + sizeof(FixedSizeRobinHoodHashTableFileHeader);
            endTablePointer = tablePointer + recordSize * slots;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte *GetRecordPointer(ulong recordIndex)
        {
            return tablePointer + recordSize * recordIndex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ushort GetDistance(byte* recordPointer)
        {
            return *(ushort*)(recordPointer + distanceOffset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetKeyPointer(byte* recordPointer)
        {
            return recordPointer + keyOffset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetKeyAsSpan(byte* keyPointer)
        {
            return new ReadOnlySpan<byte>(keyPointer, (int)keySize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        TKey GetKey(byte* keyPointer)
        {
            return *(TKey*)keyPointer;
            //return Unsafe.AsRef<TKey>(keyPointer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetValuePointer(byte* recordPointer)
        {
            return recordPointer + valueOffset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetValueAsSpan(byte *valuePointer)
        {
            return new ReadOnlySpan<byte>(valuePointer, (int)valueSize);
        }

        TValue GetValue(byte* valuePointer)
        {
            return *(TValue*)valuePointer;
            //return Unsafe.AsRef<TValue>(valuePointer);
        }

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
            headerPointer->Slots = slots;
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
                throw new ArgumentException("Mismatched RecordSize");
            }
        }

        private void CalculateOffsetsAndSizes()
        {
            keySize = (uint) Unsafe.SizeOf<TKey>();
            valueSize = (uint) Unsafe.SizeOf<TValue>();
            distanceSize = (uint) Unsafe.SizeOf<ushort>();


            byte keyAlignement = GetAlignement((uint)Unsafe.SizeOf<TKey>());
            byte valueAlignement = GetAlignement((uint)Unsafe.SizeOf<TValue>());
            byte distanceAlignement = GetAlignement((uint)Unsafe.SizeOf<ushort>());
            byte recordAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            keyOffset = 0;
            valueOffset = keyOffset + keySize + GetPadding(keyOffset + keySize, valueAlignement);
            distanceOffset = valueOffset + valueSize + GetPadding(valueOffset + valueSize, distanceAlignement);
            recordSize = distanceOffset + distanceSize + GetPadding(distanceOffset + distanceSize, recordAlignement);
            
        }

        private uint GetPadding(uint offsetWithoutPadding, byte alignement)
        {
            if (!isAligned) return 0u;
            uint twoPowerAlignement = 1u << alignement;
            uint twoPowerAlignementMinusOne = twoPowerAlignement - 1;
            // (2^alignement - (offsetWithoutPaddig) & (2^alignement - 1))) & (2^alignement -1)
            return (twoPowerAlignement - (offsetWithoutPadding & twoPowerAlignementMinusOne)) & twoPowerAlignementMinusOne;

        }

        private byte GetAlignement(uint size)
        {
            if (!isAligned) return 0;
            if (size >= 8) return 3;
            if (size >= 4) return 2;
            if (size >= 2) return 1;
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ulong GetIdealRecordIndex(TKey key)
        {
            var hash = Hashing.MetroHash64((byte*)Unsafe.AsPointer(ref key), keySize);
            return hash & mask;
        }

        public bool TryGet(TKey key, out TValue value)
        {
            var idealRecordIndex = GetIdealRecordIndex(key);
            var recordPointer = FindRecordPointer(idealRecordIndex, key);
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
            var idealRecordIndex = GetIdealRecordIndex(key);
            var recordPointer = FindRecordPointer(idealRecordIndex, key);
            if (recordPointer == null)
            {
                Add(idealRecordIndex, key, value);
            }
            else
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }

        public void Put(TKey key, TValue value)
        {
            var idealRecordIndex = GetIdealRecordIndex(key);
            var recordPointer = FindRecordPointer(idealRecordIndex, key);
            if (recordPointer == null)
            {
                Add(idealRecordIndex, key, value);
            }
            else
            {
                *(TValue*)GetValuePointer(recordPointer) = value;
                //Unsafe.AsRef<TValue>(GetValuePointer(recordPointer)) = value;
            }
        }

        private void Add(ulong idealRecordIndex, TKey key, TValue value)
        {
            throw new NotImplementedException();
        }

        private byte* FindRecordPointer(ulong idealRecordIndex, TKey key)
        {
            var recordPointer = GetRecordPointer(idealRecordIndex);
            var keyPointer = Unsafe.AsPointer(ref key);
            do
            {
                if (GetDistance(recordPointer) == 0) return null;
                if (Memory.Compare(keyPointer, GetKeyPointer(recordPointer), (int) keySize) == 0)
                {
                    return recordPointer;
                }
                recordPointer += recordSize;
            } while (recordPointer < endTablePointer);
            return null;
        }

        public bool IsDisposed { get; private set; }

        public void Dispose()
        {
            if (IsDisposed) return;
            IsDisposed = true;
            if (this.mappingSession != null) this.mappingSession.Dispose();
            if (this.memoryMapper != null) this.memoryMapper.Dispose();
        }
    }
}
