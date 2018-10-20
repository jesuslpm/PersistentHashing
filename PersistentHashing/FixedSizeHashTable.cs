using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace PersistentHashing
{
    public unsafe class FixedSizeHashTable<TKey, TValue>: IDisposable, IDictionary<TKey, TValue> where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><slot-padding>

        const int keyOffset = 0;
        int valueOffset;
        int distanceOffset;
        readonly bool isAligned;

        int keySize;
        int valueSize;
        int distanceSize;
        internal int recordSize;
        readonly long slotCount;
        readonly long mask;

        private MemoryMapper memoryMapper;
        private MemoryMappingSession mappingSession;
        private byte* fileBaseAddress;

        private FixedSizeHashTableFileHeader* headerPointer;
        internal readonly byte* tablePointer;
        internal readonly byte* endTablePointer;
        private readonly int bits;

        public int MaxDistance { get; private set; }

        private readonly Func<TKey, long> hashFunction;
        private readonly IEqualityComparer<TKey> comparer;
        private readonly IEqualityComparer<TValue> valueComparer;

        public long Count
        {
            get { return headerPointer->RecordCount; }
            private set { headerPointer->RecordCount = value; }
        }



        public FixedSizeHashTable(string filePath, long capacity, Func<TKey, long> hashFunction = null, IEqualityComparer<TKey> comparer = null,  bool isAligned = false)
        {
            this.isAligned = isAligned;
            this.hashFunction = hashFunction;
            this.comparer = comparer ?? EqualityComparer<TKey>.Default;
            this.valueComparer = EqualityComparer<TValue>.Default;
            CalculateOffsetsAndSizes();
            slotCount = (long) Bits.NextPowerOf2(capacity);
            mask = (long) slotCount - 1L;
            bits = Bits.MostSignificantBit(slotCount);

            var fileSize = (long) sizeof(FixedSizeHashTableFileHeader) +  slotCount * recordSize;
            fileSize += (Constants.AllocationGranularity - (fileSize & (Constants.AllocationGranularity - 1))) & (Constants.AllocationGranularity - 1);

            var isNew = !File.Exists(filePath);

            memoryMapper = new MemoryMapper(filePath, (long) fileSize);
            mappingSession = memoryMapper.OpenSession();

            fileBaseAddress = mappingSession.GetBaseAddress();
            headerPointer = (FixedSizeHashTableFileHeader*)fileBaseAddress;

            if (isNew)
            {
                WriteHeader();
            }
            else
            {
                ValidateHeader();
                slotCount = headerPointer->Slots;
            }
            tablePointer = fileBaseAddress + sizeof(FixedSizeHashTableFileHeader);
            endTablePointer = tablePointer + recordSize * slotCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(long slotIndex) => 
            tablePointer + recordSize * slotIndex;

        /// <summary>
        /// Returns the distance + 1 to the initial slot index.
        /// The initial slot index is the slot index where the hash maps to.
        /// distance == 0 means the slot is free. distance > 0 means the slot is occupied
        /// The distance is incremented by 1 to reserve zero value as free slot.
        /// </summary>
        /// <param name="recordPointer"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal short GetDistance(byte* recordPointer) => 
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
        internal byte* GetKeyPointer(byte* recordPointer) => 
            recordPointer + keyOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetKeyAsSpan(byte* keyPointer) => 
            new ReadOnlySpan<byte>(keyPointer, (int)keySize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TKey GetKey(byte* keyPointer) => 
            *(TKey*)keyPointer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetValuePointer(byte* recordPointer) => 
            recordPointer + valueOffset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ReadOnlySpan<byte> GetValueAsSpan(byte *valuePointer) => 
            new ReadOnlySpan<byte>(valuePointer, (int)valueSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static internal TValue GetValue(byte* valuePointer) => 
            *(TValue*)valuePointer;

        void WriteHeader()
        {
            headerPointer->IsAligned = isAligned;
            headerPointer->KeySize = keySize;
            headerPointer->Magic = FixedSizeHashTableFileHeader.MagicNumber;
            Count = 0;
            headerPointer->RecordSize = recordSize;
            headerPointer->Reserved[0] = 0;
            headerPointer->Reserved[1] = 0;
            headerPointer->Reserved[2] = 0;
            headerPointer->Slots = slotCount;
            headerPointer->ValueSize = valueSize;
        }

        void ValidateHeader()
        {
            if ( headerPointer->Magic != FixedSizeHashTableFileHeader.MagicNumber)
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
            keySize = sizeof(TKey);
            valueSize = sizeof(TValue);
            distanceSize = sizeof(short);


            int keyAlignement = GetAlignement(keySize);
            int valueAlignement = GetAlignement(valueSize);
            int distanceAlignement = GetAlignement(distanceSize);
            int slotAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            //keyOffset = 0;
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
        private long GetInitialSlotIndex(TKey key)
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

        public bool TryGetValue(TKey key, out TValue value)
        {
            var initialSlotIndex = GetInitialSlotIndex(key);
            var recordPointer = FindRecord(initialSlotIndex, key);
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
            if (!TryAdd(key, value))
            {
                throw new ArgumentException($"An element with the same key {key} already exists");
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            var initialSlotIndex = GetInitialSlotIndex(key);
            var recordPointer = FindRecord(initialSlotIndex, key);
            if (recordPointer == null)
            {
                RobinHoodAdd(initialSlotIndex, key, value);
                
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool ContainsKey(TKey key)
        {
            return FindRecord(GetInitialSlotIndex(key), key) != null;
        }

        public void Put(TKey key, TValue value)
        {
            var initialSlotIndex = GetInitialSlotIndex(key);
            var recordPointer = FindRecord(initialSlotIndex, key);
            if (recordPointer == null)
            {
                RobinHoodAdd(initialSlotIndex, key, value);
            }
            else
            {
                SetValue(recordPointer, value);
                //Unsafe.AsRef<TValue>(GetValuePointer(recordPointer)) = value;
            }
        }

        private void RobinHoodAdd(long initialSlotIndex, TKey key, TValue value)
        {
            byte* recordPointer = GetRecordPointer(initialSlotIndex);
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
                    Count = Count + 1;
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

        public void Delete(TKey key)
        {
            if (!Remove(key))
            {
                throw new ArgumentException($"key {key} not found");
            }
        }

        public bool Remove(TKey key)
        {
            byte* emptyRecordPointer = FindRecord(GetInitialSlotIndex(key), key);
            if (emptyRecordPointer == null)
            {
                return false;
            }
            byte* currentRecordPointer = emptyRecordPointer;
            while (true)
            {
                /*
                 * shift backward all entries following the entry to delete until either find an empty slot, 
                 * or a record with a distance of 1  
                 */
                currentRecordPointer += recordSize;
                if (currentRecordPointer >= endTablePointer) // start from begining when reaching the end
                {
                    currentRecordPointer = tablePointer;
                }
                short distance = GetDistance(currentRecordPointer);
                if (distance <= 1)
                {
                    SetDistance(emptyRecordPointer, 0);
                    return true;
                }
                SetKey(emptyRecordPointer, GetKey(currentRecordPointer));
                SetValue(emptyRecordPointer, GetValue(currentRecordPointer));
                SetDistance(emptyRecordPointer, (short)(distance - 1));
                emptyRecordPointer = currentRecordPointer;
            }
        }

        private byte* FindRecord(long initialSlotIndex, TKey key)
        {
            byte* recordPointer = GetRecordPointer(initialSlotIndex);
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

        public ICollection<TKey> Keys => new FixedSizeHashTableKeyCollection<TKey, TValue>(this);

        public ICollection<TValue> Values => new FixedSizeHashTableValueCollection<TKey, TValue>(this);

        int ICollection<KeyValuePair<TKey, TValue>>.Count => (int) Count;

        public bool IsReadOnly => false;

        public TValue this[TKey key]
        {
            get
            {
                if (!TryGetValue(key, out TValue value))
                {
                    throw new ArgumentException($"Key {key} not found.");
                }
                return value;
            }
            set
            {
                Put(key, value);
            }
        }

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

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            Memory.ZeroMemory(new IntPtr(tablePointer), new IntPtr(recordSize * slotCount));
            Count = 0;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out TValue value))
            {
                return valueComparer.Equals(item.Value, value);
            }
            return false;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0) throw new ArgumentOutOfRangeException(nameof(arrayIndex), "arrayIndex parameter must be greater than zero");
            if (this.Count > array.Length - arrayIndex) throw new ArgumentException("The array has not enough space to hold all items");
            foreach (var keyValue in this)
            {
                array[arrayIndex++] = keyValue;
            }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }


        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new FixedSizeHashTableRecordEnumerator<TKey, TValue>(this);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
