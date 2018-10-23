using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public unsafe class StaticFixedSizeHashTable<TKey, TValue>: IDisposable, IDictionary<TKey, TValue> where TKey:unmanaged where TValue:unmanaged
    {
        // <key><value-padding><value><distance padding><distance16><record-padding>

        public const short MaxAllowedDistance = 512;
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

        private StaticFixedSizeHashTableFileHeader* headerPointer;
        internal readonly byte* tablePointer;
        internal readonly byte* endTablePointer;
        private readonly int bits;

        public readonly ThreadSafety ThreadSafety;
        private int syncObjectCount;
        private object[] syncObjects;
        private int maxLocksPerOperation;
        private long slotsPerSyncObject;
        private int slotsPerSyncObjectBits;
        private long slotsPerSyncObjectMask;

        

        /// <summary>
        /// Max distance ever seen in the hash table. 
        /// MaxDistance is updated only on adding, It is not uptaded on removing.
        /// </summary>
        // MaxDistance starts with 0 while internal distance starts with 1. So, it is the real max distance.
        public int MaxDistance => headerPointer->MaxDistance;

        //Note that float casts are not redundant as VS says.
        public float LoadFactor => (float)Count / (float)slotCount;
        public float MeanDistance => (float)headerPointer->DistanceSum / (float)Count;

        private readonly Func<TKey, long> hashFunction;
        private readonly IEqualityComparer<TKey> comparer;
        private readonly IEqualityComparer<TValue> valueComparer;
        private readonly long Capacity;


        public long Count
        {
            get { return headerPointer->RecordCount; }
            private set { headerPointer->RecordCount = value; }
        }



        public StaticFixedSizeHashTable(string filePath, long capacity, ThreadSafety threadSafety, Func<TKey, long> hashFunction = null, IEqualityComparer<TKey> comparer = null,  bool isAligned = false)
        {
            this.isAligned = isAligned;
            this.hashFunction = hashFunction;
            this.comparer = comparer ?? EqualityComparer<TKey>.Default;
            this.valueComparer = EqualityComparer<TValue>.Default;
            CalculateOffsetsAndSizes();
            slotCount = Math.Max(capacity + capacity/8 + capacity/16, 4);
            slotCount = Bits.IsPowerOfTwo(slotCount) ? slotCount : Bits.NextPowerOf2(slotCount);
            this.Capacity = slotCount - slotCount/8 - capacity/16; // conservative max load factor = 81.25%
            mask = slotCount - 1L;
            bits = Bits.MostSignificantBit(slotCount);
            this.ThreadSafety = threadSafety;

            var fileSize = (long) sizeof(StaticFixedSizeHashTableFileHeader) +  slotCount * recordSize;
            fileSize += (Constants.AllocationGranularity - (fileSize & (Constants.AllocationGranularity - 1))) & (Constants.AllocationGranularity - 1);

            var isNew = !File.Exists(filePath);

            memoryMapper = new MemoryMapper(filePath, (long) fileSize);
            mappingSession = memoryMapper.OpenSession();

            fileBaseAddress = mappingSession.GetBaseAddress();
            headerPointer = (StaticFixedSizeHashTableFileHeader*)fileBaseAddress;


            if (isNew)
            {
                InitializeHeader();
            }
            else
            {
                ValidateHeader();
                slotCount = headerPointer->SlotCount;
            }
            tablePointer = fileBaseAddress + sizeof(StaticFixedSizeHashTableFileHeader);
            endTablePointer = tablePointer + recordSize * slotCount;
        }

        private void InitializeSynchronization()
        {
            if (this.ThreadSafety == ThreadSafety.Unsafe) return;
            /*
             According to the Birthday problem and using the Square approximation
             p(n) = n^2/2/m. where p is the probalitity, n is the number of people and m is the number of days in a year.
             m = n^2/2/p(n)
             syncObjectCount maps to the number of days in a year (m), 
             numer of threads accessing hash table maps to number of people (n).
             we are guessing number of threads = Environment.ProcessorCount * 2  
             and we want a probability of 1/8 that a thread gets blocked.

             we are constraining syncObjectCount to be between 64 and 8192
            */
            syncObjectCount = Math.Min(Math.Max( Environment.ProcessorCount * Environment.ProcessorCount * 4, 64), 8192);

            // but we need a power of two
            if (Bits.IsPowerOfTwo(syncObjectCount) == false) syncObjectCount = Bits.NextPowerOf2(syncObjectCount);

            // we want at least 4 slots per sync object, so that most of the time (when distance <= 3) we only need to lock one sync object
            slotsPerSyncObject = Math.Max(slotCount / syncObjectCount, 4);

            // We need to use an array of maxLocksPerOperation booleans in each operation.
            // We are going to stackalloc that array, therefore it should be small, a maximum of 256 seems to be reasonable.
            // and it must be at least 1.
            maxLocksPerOperation = Math.Max(Math.Min((int) (MaxAllowedDistance / slotsPerSyncObject),  256), 1);

            // recalc values with constraints applied 
            slotsPerSyncObject = MaxAllowedDistance / maxLocksPerOperation;
            syncObjectCount = (int)(slotCount / slotsPerSyncObject);

            Debug.Assert(Bits.IsPowerOfTwo(slotsPerSyncObject));
            slotsPerSyncObjectMask = slotsPerSyncObject - 1;
            slotsPerSyncObjectBits = Bits.MostSignificantBit(slotsPerSyncObject);

            // we need to cover MaxAllowedDistante slots in maxLocksPerOperation locks
            maxLocksPerOperation = maxLocksPerOperation + 1;
            Debug.Assert(maxLocksPerOperation * slotsPerSyncObject > MaxAllowedDistance);

            syncObjects = new object[syncObjectCount];
            for (int i = 0; i < syncObjectCount; i++) syncObjects[i] = new object();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        byte* GetRecordPointer(long slotIndex) => 
            tablePointer + recordSize * slotIndex;

        /// <summary>
        /// Returns the distance + 1 to the initial slot index.
        /// The initial slot index is the slot index where the hash maps to.
        /// distance == 0 means the slot is empty. distance > 0 means the slot is occupied
        /// The distance is incremented by 1 to reserve zero value as empty slot.
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

        void InitializeHeader()
        {
            headerPointer->IsAligned = isAligned;
            headerPointer->DistanceSum = 0;
            headerPointer->KeySize = keySize;
            headerPointer->Magic = StaticFixedSizeHashTableFileHeader.MagicNumber;
            headerPointer->MaxDistance = 0;
            headerPointer->RecordCount = 0;
            headerPointer->RecordSize = recordSize;
            headerPointer->Reserved[0] = 0;
            headerPointer->SlotCount = slotCount;
            headerPointer->ValueSize = valueSize;
        }

        void ValidateHeader()
        {
            if ( headerPointer->Magic != StaticFixedSizeHashTableFileHeader.MagicNumber)
            {
                throw new FormatException($"This is not a {nameof(StaticFixedSizeHashTable<TKey, TValue>)} file");
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

        /// <summary>
        /// Returns the intial slot index corresponding to the key. 
        /// It is the index that the key hash maps to.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
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
            }
        }

        private void RobinHoodAdd(long initialSlotIndex, TKey key, TValue value)
        {
            byte* recordPointer = GetRecordPointer(initialSlotIndex);
            short distance = 1; //start with 1 because 0 is reserved for empty slots.
            while(true)
            {
                short currentRecordDistance = GetDistance(recordPointer);
                if (currentRecordDistance == 0) // found empty slot
                {
                    SetKey(recordPointer, key);
                    SetValue(recordPointer, value);
                    SetDistance(recordPointer, distance);

                    if (headerPointer->MaxDistance < distance -1) headerPointer->MaxDistance = distance - 1;
                    headerPointer->DistanceSum += distance - 1;
                    headerPointer->RecordCount++;
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

                    headerPointer->DistanceSum += distance - currentRecordDistance;

                    key = tempKey;
                    value = tempValue;
                    distance = currentRecordDistance;
                }
                if (distance++ > MaxAllowedDistance)
                {
                    throw new InvalidOperationException("Reached MaxAllowedDistance");
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
            headerPointer->DistanceSum -= GetDistance(emptyRecordPointer) - 1;
            byte* currentRecordPointer = emptyRecordPointer;
            while (true)
            {
                /*
                 * shift backward all entries following the entry to delete until either find an empty slot, 
                 * or a record with a distance of 0 (1 in our case)
                 * http://codecapsule.com/2013/11/17/robin-hood-hashing-backward-shift-deletion/
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
                headerPointer->DistanceSum--;
                emptyRecordPointer = currentRecordPointer;
            }
        }

        private byte* FindRecord(long initialSlotIndex, TKey key, bool* locks = null)
        {
            byte* recordPointer = GetRecordPointer(initialSlotIndex);
            int distance = 1;
            long remainingSlotsInSyncObject = 0;
            long slotIndex = initialSlotIndex;
            int lockIndex = 0;
            while (true)
            {
                LockIfNeeded(locks, ref remainingSlotsInSyncObject, slotIndex, ref lockIndex);
                if (GetDistance(recordPointer) == 0) return null;
                if (comparer.Equals(key, GetKey(GetKeyPointer(recordPointer))))
                {
                    return recordPointer;
                }
                recordPointer += recordSize;
                slotIndex++;
                if (recordPointer >= endTablePointer)
                {
                    recordPointer = tablePointer;
                    slotIndex = 0;
                }
                if (distance++ > MaxAllowedDistance) throw new InvalidOperationException("Reached MaxAllowedDistance");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LockIfNeeded(bool* locks, ref long remainingSlotsInSyncObject, long slotIndex, ref int lockIndex)
        {
            if (locks != null)
            {
                if (remainingSlotsInSyncObject == 0)
                {
                    if (locks[lockIndex]==false) Monitor.Enter(syncObjects[slotCount << slotsPerSyncObjectBits], ref locks[lockIndex]);
                    lockIndex++;
                    remainingSlotsInSyncObject = slotsPerSyncObject - slotIndex & slotsPerSyncObjectMask - 1;
                }
                else
                {
                    remainingSlotsInSyncObject--;
                }
            }
        }

        public bool IsDisposed { get; private set; }

        public ICollection<TKey> Keys => new StaticFixedSizeHashTableKeyCollection<TKey, TValue>(this);

        public ICollection<TValue> Values => new StaticFixedSizeHashTableValueCollection<TKey, TValue>(this);

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
            if (Count > 0)
            {
                Memory.ZeroMemory(tablePointer, recordSize * slotCount);
                Count = 0;
            }
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
            return new StaticFixedSizeHashTableRecordEnumerator<TKey, TValue>(this);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
       
    }
}
