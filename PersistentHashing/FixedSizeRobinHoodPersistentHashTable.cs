﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace PersistentHashing
{
    public unsafe class FixedSizeRobinHoodPersistentHashTable<TKey, TValue>: IDisposable where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><slot-padding>

        int keyOffset;
        int valueOffset;
        int distanceOffset;
        readonly bool isAligned;

        int keySize;
        int valueSize;
        int distanceSize;
        int recordSize;
        readonly long slotCount;
        readonly long mask;

        private MemoryMapper memoryMapper;
        private MemoryMappingSession mappingSession;
        private byte* fileBaseAddress;

        private FixedSizeRobinHoodHashTableFileHeader* headerPointer;
        private readonly byte* tablePointer;
        private readonly byte* endTablePointer;
        private readonly int bits;

        public int MaxDistance { get; private set; }

        private readonly Func<TKey, long> hashFunction;
        private readonly IEqualityComparer<TKey> comparer;
        

        private const int AllocationGranularity = 64 * 1024;

        public FixedSizeRobinHoodPersistentHashTable(string filePath, long capacity, Func<TKey, long> hashFunction = null, IEqualityComparer<TKey> comparer = null,  bool isAligned = false)
        {
            this.isAligned = isAligned;
            this.hashFunction = hashFunction;
            this.comparer = comparer ?? EqualityComparer<TKey>.Default;
            CalculateOffsetsAndSizes();
            slotCount = (long) Bits.NextPowerOf2(capacity);
            mask = (long) slotCount - 1L;
            bits = Bits.MostSignificantBit(slotCount);

            var fileSize = (long) sizeof(FixedSizeRobinHoodHashTableFileHeader) +  slotCount * recordSize;
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
        byte* GetRecordPointer(long slotIndex) => 
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
        short GetDistance(byte* recordPointer) => 
            *(short*)(recordPointer + distanceOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetDistance(byte* recordPointer, short distance)
        {
            *(short*)(recordPointer + distanceOffset) = distance;
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
            keySize = (int) sizeof(TKey);
            valueSize = (int) sizeof(TValue);
            distanceSize = sizeof(short);


            int keyAlignement = GetAlignement(keySize);
            int valueAlignement = GetAlignement(valueSize);
            int distanceAlignement = GetAlignement(distanceSize);
            int slotAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            keyOffset = 0;
            valueOffset = keyOffset + keySize + GetPadding(keyOffset + keySize, valueAlignement);
            distanceOffset = valueOffset + valueSize + GetPadding(valueOffset + valueSize, distanceAlignement);
            recordSize = distanceOffset + distanceSize + GetPadding(distanceOffset + distanceSize, slotAlignement);
        }

        private int GetPadding(int offsetWithoutPadding, int alignement)
        {
            if (!isAligned) return 0;
            int alignementMinusOne = alignement - 1;
            // (alignement - offsetWithoutPadding % alignement) % alignement
            return (alignement - (offsetWithoutPadding & alignementMinusOne)) & alignementMinusOne;
        }

        private int GetAlignement(int size)
        {
            if (!isAligned) return 1;
            if (size >= 8) return 8;
            if (size >= 4) return 4;
            if (size >= 2) return 2;
            return 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetIdealSlotIndex(TKey key)
        {
            if (hashFunction == null)
            {
                return Hashing.FastHash64((byte*)&key, keySize) & mask;
            }
            else
            {
                //return hashFunction(key) * 11400714819323198485LU & mask;
                return hashFunction(key) & mask;
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            var idealSlotIndex = GetIdealSlotIndex(key);
            var recordPointer = FindRecord(idealSlotIndex, key);
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
            var recordPointer = FindRecord(idealSlotIndex, key);
            if (recordPointer == null)
            {
                RobinHoodAdd(idealSlotIndex, key, value);
            }
            else
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            var idealSlotIndex = GetIdealSlotIndex(key);
            var recordPointer = FindRecord(idealSlotIndex, key);
            if (recordPointer == null)
            {
                RobinHoodAdd(idealSlotIndex, key, value);
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool ContainsKey(TKey key)
        {
            return FindRecord(GetIdealSlotIndex(key), key) != null;
        }

        public void Put(TKey key, TValue value)
        {
            var idealSlotIndex = GetIdealSlotIndex(key);
            var recordPointer = FindRecord(idealSlotIndex, key);
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

        private void RobinHoodAdd(long idealSlotIndex, TKey key, TValue value)
        {
            byte* recordPointer = GetRecordPointer(idealSlotIndex);
            short distance = 1; //start with 1 because 0 is reserved for free slots.
            for (;;)
            {
                short currentRecordDistance = GetDistance(recordPointer);
                if (currentRecordDistance == 0) // found free slot
                {
                    SetKey(recordPointer, key);
                    SetValue(recordPointer, value);
                    SetDistance(recordPointer, distance);
                    if (MaxDistance < distance) MaxDistance = distance;
                    return;
                }
                else if (currentRecordDistance < distance) // found richer record
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
                if (recordPointer >= endTablePointer) // start from begining when reaching the end
                {
                    recordPointer = tablePointer;
                }
            }
        }

        private byte* FindRecord(long idealSlotIndex, TKey key)
        {
            byte* recordPointer = GetRecordPointer(idealSlotIndex);
            bool isEndTableReached = false;
            while (true)
            {
                if (GetDistance(recordPointer) == 0) return null;
                if (comparer.Equals(key, GetKey(GetKeyPointer(recordPointer))))
                {
                    return recordPointer;
                }
                recordPointer += recordSize;
                if (recordPointer >= endTablePointer)
                {
                    if (isEndTableReached) return null;
                    recordPointer = tablePointer;
                    isEndTableReached = true;
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
