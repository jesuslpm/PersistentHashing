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
    public unsafe abstract class StaticHashTableStore<TKey, TValue>: IDisposable
    {
        // <key><value-padding><value><distance padding><distance16><record-padding>

        internal StaticHashTableConfig<TKey, TValue> config;

        /// <summary>
        /// Max distance ever seen in the hash table. 
        /// MaxDistance is updated only on adding, It is not uptaded on removing.
        /// It' only updated when building with DEBUG simbol defined.
        /// </summary>
        // MaxDistance starts with 0 while internal distance starts with 1. So, it is the real max distance.
        public int MaxDistance => config.HeaderPointer->MaxDistance;

        //Note that float casts are not redundant as VS says.
        public float LoadFactor => (float)Count / (float)config.SlotCount;

        // this is only updated when building with DEBUG simbol defined.
        public float MeanDistance => (float)config.HeaderPointer->DistanceSum / (float)Count;

        public long Capacity => config.Capacity;

        public long Count => config.HeaderPointer->RecordCount;

        public string HashTableFilePath => config.HashTableFilePath;
        public string DataFilePath => config.DataFilePath;

        private bool isInitialized;


        public StaticHashTableStore(string filePathWithoutExtension, long capacity, HashTableOptions<TKey, TValue> options = null)
        {
            config.HashTableFilePath = filePathWithoutExtension + ".HashTable";
            config.DataFilePath = filePathWithoutExtension + ".DataFile";
            config.IsNew = !File.Exists(config.HashTableFilePath);
            

            // SlotCount 1, 2, 4, and 8 are edge cases that is not worth to support. So min slot count is 16, it doesn't show "anomalies".
            // (capacity + 6) / 7 is Ceil(capacity FloatDiv 7)
            // we want a MaxLoadFactor = Capacity/SlotCount = 87.5% this is why we set SlotCount = capacity + capacity/7 => 7 * slotCount = 8 * capacity => capacity/SlotCount = 7/8 = 87.5%
            config.SlotCount = Math.Max(capacity + (capacity + 6)/7, 16);
            config.SlotCount = Bits.IsPowerOfTwo(config.SlotCount) ? config.SlotCount : Bits.NextPowerOf2(config.SlotCount);
            config.SlotBits = Bits.MostSignificantBit(config.SlotCount);
            config.Capacity = (config.SlotCount / 8) * 7; // max load factor = 7/8 = 87.5%
            config.HashMask = config.SlotCount - 1L;

            if (options != null)
            {
                config.HashFunction = options.HashFunction;
                config.IsAligned = options.IsAligned;
                config.KeyComparer = options.KeyComparer;
                config.ValueComparer = options.ValueComparer;
            }
            

            /*
             * We use System.Threading.Monitor To achieve synchronization
             * The table is divided into equal sized chunks of slots.
             * The numer of chunks is a power of two that ranges from 8 to 8192
             * We use an array of sync objects with one sync object per chunk.
             * The sync object associated with a chunk is locked when accesing slots in the chunk.
             * If the record distance is greater than chunk size, more than one sync object will be locked.
             * But we never lock more than 8 sync objects in a single operation.
             */


            /*
             According to the Birthday problem and using the Square approximation
             p(n) = n^2/2/m. where p is the probalitity that at least two people have the same birthday, 
             n is the number of people and m is the number of days in a year.
             m = n^2/2/p(n)
             ChunkCount which is equals to SyncObjects.Length maps to the number of days in a year (m), 
             numer of threads accessing hash table maps to number of people (n).
             we are guessing number of threads = Environment.ProcessorCount * 2  
             and we want a probability of 1/8 that a thread gets blocked.
            
            */
            config.ChunkCount = Math.Min(Environment.ProcessorCount * Environment.ProcessorCount * 16, 8192);

            // but we need a power of two
            if (Bits.IsPowerOfTwo(config.ChunkCount) == false) config.ChunkCount = Math.Min(Bits.NextPowerOf2(config.ChunkCount), 8192);


            // We impose the following constraint: max locks per operation cannot be greater than 8
            // We use one long as an array of 8 bools to keep track of locked sync objects
            // we want at least 64 slots per chunk when SlotCount >= 512, for smaller tables SlotCount/8
            // with 8 locks and 64 slots per chunk, we can cover 8*64 = 512 slots and (8 - 1)* 64 = 448 MaxAllowedDistance  
            // Most of the time, when distance < 64, we only need to lock one sync object
            int minChunkSize = (int)Math.Min(config.SlotCount / 8, 64);
            config.ChunkSize = Math.Max(config.SlotCount / config.ChunkCount, minChunkSize);
            config.ChunkMask = config.ChunkSize - 1;
            config.ChunkBits = Bits.MostSignificantBit(config.ChunkSize);

            // recalc with constraints applied 
            config.ChunkCount = (int)(config.SlotCount / config.ChunkSize);

            // As said, max locks per operation cannot be greater than 8. 
            // We must satisfy (MaxLocksPerOperation - 1) * ChunkSize >= MaxAllowedDistance,
            // Threrefore MaxAllowedDistance cannot be greater than 7 * ChunkSize 
            // But this value can be huge, So we want to constraint MaxAllowedDistance to a smaller value.
            // It is known that max distance grows very slowly (logarithmicaly) with slot count. We guess: max distance = a + k * log2 (slotCount)
            // (4.3 In Robin Hood hashing, the maximum DIB increases with the table size: http://codecapsule.com/2014/05/07/implementing-a-key-value-store-part-6-open-addressing-hash-tables/)
            // for SlotCount = 512, the maximum MaxAllowedDistance without deadlocks is (config.ChunkCount - 2) * ChunkSize = 6*64 = 384.
            // Doing the math we get a=6, k=42, log2(slotCount) = config.SlotCountBits. 
            // 42 is "The Answer to the Ultimate Question of Life, the Universe, and Everything". Is it just coincidence?
            config.MaxAllowedDistance = (int)Math.Min(7 * config.ChunkSize, 6 + 42 * config.SlotBits);

            // We nedd to adjust MaxAllowedDistance to avoid deadlocks. A deadlock will occur when a thread A start locking the first chunk,
            // another thread B start locking the last chunk, then B tries to lock the first chunk. If A reaches the
            // last chunk we have a dealock. Threfore MaxAllowedDistance must not span more than config.ChunkCount - 2 chunks.
            config.MaxAllowedDistance = (int)Math.Min((config.ChunkCount - 2) * config.ChunkSize, config.MaxAllowedDistance);


            // We use an array of max locks per operation booleans to keep track of locked sync objects
            // (config.MaxAllowedDistance << config.ChunkBits) + ((config.MaxAllowedDistance & config.ChunkMask) == 0 ? 1 : 2) this weird thing is ceil(MaxAllowedDistance FloatDiv ChunkSize) + 1
            config.MaxLocksPerOperation = Math.Max((int)((config.MaxAllowedDistance << config.ChunkBits) + ((config.MaxAllowedDistance & config.ChunkMask) == 0 ? 1 : 2)), 2);

            Debug.Assert(Bits.IsPowerOfTwo(config.ChunkSize));
            Debug.Assert(config.MaxLocksPerOperation > 1 && config.MaxLocksPerOperation <= 8);

            // MaxAllowedDistance is covered with MaxLocksPerOperation locks
            Debug.Assert((config.MaxLocksPerOperation - 1) * config.ChunkSize >= config.MaxAllowedDistance);

            // MaxAllowedDistance is reached before deadlocking.
            Debug.Assert(config.MaxAllowedDistance <= (config.ChunkCount - 2) * config.ChunkSize);

            if (config.IsThreadSafe)
            {
                config.SyncObjects = new SyncObject[config.ChunkCount];
                for (int i = 0; i < config.ChunkCount; i++) config.SyncObjects[i] = new SyncObject();
            }
        }


        protected void InitializeHeader()
        {
            config.HeaderPointer->IsAligned = config.IsAligned;
            config.HeaderPointer->DistanceSum = 0;
            config.HeaderPointer->KeySize = config.KeySize;
            config.HeaderPointer->Magic = StaticFixedSizeHashTableFileHeader.MagicNumber;
            config.HeaderPointer->MaxDistance = 0;
            config.HeaderPointer->RecordCount = 0;
            config.HeaderPointer->RecordSize = config.RecordSize;
            config.HeaderPointer->Reserved[0] = 0;
            config.HeaderPointer->SlotCount = config.SlotCount;
            config.HeaderPointer->ValueSize = config.ValueSize;
        }

        protected void ValidateHeader()
        {
            if ( config.HeaderPointer->Magic != StaticFixedSizeHashTableFileHeader.MagicNumber)
            {
                throw new FormatException($"This is not a {nameof(StaticHashTableStore<TKey, TValue>)} file");
            }
            if (config.HeaderPointer->IsAligned != config.IsAligned)
            {
                throw new ArgumentException("Mismatched alignement");
            }
            if (config.HeaderPointer->KeySize != config.KeySize)
            {
                throw new ArgumentException("Mismatched config.KeySize");
            }
            if (config.HeaderPointer->ValueSize != config.ValueSize)
            {
                throw new ArgumentException("Mismatched ValueSize");
            }
            if (config.HeaderPointer->RecordSize != config.RecordSize)
            {
                throw new ArgumentException("Mismatched SlotSize");
            }
        }


        protected int GetPadding(int offsetWithoutPadding, int alignement)
        {
            if (!config.IsAligned) return 0;
            int alignementMinusOne = alignement - 1;
            // (alignement - offsetWithoutPadding % alignement) % alignement
            return (alignement - (offsetWithoutPadding & alignementMinusOne)) & alignementMinusOne;
        }

        protected int GetAlignement(int size)
        {
            if (!config.IsAligned) return 1;
            if (size >= 8) return 8;
            if (size >= 4) return 4;
            if (size >= 2) return 2;
            return 1;
        }

        public void WarmUp()
        {
            config.TableMappingSession.WarmUp();
        }


        public bool IsDisposed { get; private set; }

 
        public virtual void Dispose()
        {
            if (IsDisposed) return;
            IsDisposed = true;
            if (config.TableMemoryMapper != null) config.TableMemoryMapper.Dispose();
            if (config.DataFile != null) config.DataFile.Dispose();
        }

        public virtual void Flush()
        {
            config.TableMemoryMapper.Flush();
            if (config.DataFile != null) config.DataFile.Flush();
        }

        public abstract void Initialize();
       
    }
}
