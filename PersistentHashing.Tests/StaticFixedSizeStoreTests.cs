using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using PersistentHashing;
using System.IO;

namespace PersistentHashing.Tests
{
    public unsafe class StaticFixedSizeStoreTests
    {
        StaticFixedSizeStore<long, long> store;


        [Fact]
        public void ShouldCreateFileIfNotExistsAndTheFileShouldBeDeletableAfterDispose()
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, 8, key => key))
            {
                Assert.Equal(filePath, store.config.HashTableFilePath);

                // the file is not created in the constructor
                Assert.False(File.Exists(filePath));

                // the file should be created when calling EnsureInitialized
                store.EnsureInitialized();
                Assert.True(File.Exists(filePath), "The file is not created");
            }
            // we should be able to delete the file after disposing the store
            File.Delete(filePath);
            
        }

        [Fact]
        public void ReopeningFileWithDifferentCapacityShouldNotChangeSlotCount()
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            long slotCount;
            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, 448, key => key))
            {
                store.EnsureInitialized();
                slotCount = store.config.SlotCount;
            }
            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, 1792, key => key))
            {
                store.EnsureInitialized();
                Assert.Equal(slotCount, store.config.SlotCount);
            }
            File.Delete(filePath);
        }

        [Fact]
        public void TinyTableShouldBeCorrectlyConfigured()
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            Func<long, long> hashFunction = key => key;
            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, 8, hashFunction))
            {
                store.EnsureInitialized();
                Assert.Equal(14, store.config.Capacity);
                Assert.Equal(1, store.config.ChunkBits);
                Assert.Equal(8, store.config.ChunkCount);
                Assert.Equal(1, store.config.ChunkMask);
                Assert.Equal(2, store.config.ChunkSize);
                Assert.Equal(filePathWithoutExtension + ".DataFile", store.config.DataFilePath);
                Assert.Null(store.config.DataFile);
                Assert.Equal( (ulong) (store.config.TablePointer + store.config.RecordSize * store.config.SlotCount), (ulong) store.config.EndTablePointer);
                Assert.Equal(hashFunction, store.config.HashFunction);
                Assert.Equal(15, store.config.HashMask);
                Assert.Equal(filePath, store.config.HashTableFilePath);
                
                Assert.Equal( (ulong) store.config.TableFileBaseAddress , (ulong)store.config.HeaderPointer);
                Assert.True(store.config.IsNew);
                Assert.False(store.config.IsThreadSafe);
                Assert.Equal(store.config.KeyComparer, EqualityComparer<long>.Default);
                Assert.Equal(12, store.config.MaxAllowedDistance);
                Assert.Equal(7, store.config.MaxLocksPerOperation);
                Assert.Equal(18, store.config.RecordSize);
                Assert.Equal(4, store.config.SlotBits);
                Assert.Equal(16, store.config.SlotCount);
                Assert.Null(store.config.SyncObjects);
                Assert.Equal((ulong)store.config.TableMappingSession.GetBaseAddress(), (ulong)store.config.TableFileBaseAddress);
                Assert.NotNull(store.config.TableMappingSession);
                Assert.NotNull(store.config.TableMemoryMapper);
                Assert.Equal((ulong)(store.config.TableFileBaseAddress + sizeof(StaticHashTableFileHeader)), (ulong)store.config.TablePointer);
                Assert.Equal(EqualityComparer<long>.Default, store.config.ValueComparer);

                Assert.Equal(store.Capacity, store.config.Capacity);
                Assert.Equal(0, store.Count);
                Assert.Equal(store.config.DataFilePath, store.DataFilePath);
                Assert.Equal(store.config.HashTableFilePath, store.HashTableFilePath);
                Assert.False(store.IsDisposed);
                Assert.Equal(0.0, store.LoadFactor);
                Assert.Equal(0, store.MaxDistance);
                Assert.Equal(float.NaN, store.MeanDistance);

                Assert.True(Bits.IsPowerOfTwo(store.config.ChunkSize), "ChunkSize must be a power of two");
                Assert.True(store.config.MaxLocksPerOperation > 1 && store.config.MaxLocksPerOperation <= 8, "MaxLocsPerOperation must be between 1 and 8");
                Assert.True((store.config.MaxLocksPerOperation - 1) * store.config.ChunkSize >= store.config.MaxAllowedDistance, "MaxAllowedDistance must be covered with MaxLocksPerOperation locks");
                Assert.True(store.config.MaxAllowedDistance <= (store.config.ChunkCount - 2) * store.config.ChunkSize, "MaxAllowedDistance must be reached before deadlocking.");


                store.config.IsThreadSafe = true;
                store.EnsureInitialized();
                Assert.NotNull(store.config.SyncObjects);
                Assert.Equal(store.config.ChunkCount, store.config.SyncObjects.Length);

                Assert.True((ulong)store.config.EndTablePointer - (ulong)store.config.TableFileBaseAddress <= (ulong) store.config.TableMemoryMapper.Length, "There is not enough space in the file for the table");
                Assert.True((store.config.TableMemoryMapper.fs.Length & Constants.AllocationGranularityMask) == 0, "The file length must be a multiple of AllocationGranulariry");


            }
            // we should be able to delete the file after disposing the store
            File.Delete(filePath);

        }

        [Fact]
        public void SmallTableShouldBeCorrectlyConfigured()
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            Func<long, long> hashFunction = key => key;
            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, 448, hashFunction))
            {
                store.EnsureInitialized();
                Assert.Equal(448, store.config.Capacity);
                Assert.Equal(6, store.config.ChunkBits);
                Assert.Equal(8, store.config.ChunkCount);
                Assert.Equal(63, store.config.ChunkMask);
                Assert.Equal(64, store.config.ChunkSize);
                Assert.Equal(filePathWithoutExtension + ".DataFile", store.config.DataFilePath);
                Assert.Null(store.config.DataFile);
                Assert.Equal((ulong)(store.config.TablePointer + store.config.RecordSize * store.config.SlotCount), (ulong)store.config.EndTablePointer);
                Assert.Equal(hashFunction, store.config.HashFunction);
                Assert.Equal(511, store.config.HashMask);
                Assert.Equal(filePath, store.config.HashTableFilePath);

                Assert.Equal((ulong)store.config.TableFileBaseAddress, (ulong)store.config.HeaderPointer);
                Assert.True(store.config.IsNew);
                Assert.False(store.config.IsThreadSafe);
                Assert.Equal(store.config.KeyComparer, EqualityComparer<long>.Default);
                Assert.Equal(384, store.config.MaxAllowedDistance);
                Assert.Equal(7, store.config.MaxLocksPerOperation);
                Assert.Equal(18, store.config.RecordSize);
                Assert.Equal(9, store.config.SlotBits);
                Assert.Equal(512, store.config.SlotCount);
                Assert.Null(store.config.SyncObjects);
                Assert.Equal((ulong)store.config.TableMappingSession.GetBaseAddress(), (ulong)store.config.TableFileBaseAddress);
                Assert.NotNull(store.config.TableMappingSession);
                Assert.NotNull(store.config.TableMemoryMapper);
                Assert.Equal((ulong)(store.config.TableFileBaseAddress + sizeof(StaticHashTableFileHeader)), (ulong)store.config.TablePointer);
                Assert.Equal(EqualityComparer<long>.Default, store.config.ValueComparer);

                Assert.Equal(store.Capacity, store.config.Capacity);
                Assert.Equal(0, store.Count);
                Assert.Equal(store.config.DataFilePath, store.DataFilePath);
                Assert.Equal(store.config.HashTableFilePath, store.HashTableFilePath);
                Assert.False(store.IsDisposed);
                Assert.Equal(0.0, store.LoadFactor);
                Assert.Equal(0, store.MaxDistance);
                Assert.Equal(float.NaN, store.MeanDistance);

                Assert.True(Bits.IsPowerOfTwo(store.config.ChunkSize), "ChunkSize must be a power of two");
                Assert.True(store.config.MaxLocksPerOperation > 1 && store.config.MaxLocksPerOperation <= 8, "MaxLocsPerOperation must be between 1 and 8");
                Assert.True((store.config.MaxLocksPerOperation - 1) * store.config.ChunkSize >= store.config.MaxAllowedDistance, "MaxAllowedDistance must be covered with MaxLocksPerOperation locks");
                Assert.True(store.config.MaxAllowedDistance <= (store.config.ChunkCount - 2) * store.config.ChunkSize, "MaxAllowedDistance must be reached before deadlocking.");


                store.config.IsThreadSafe = true;
                store.EnsureInitialized();
                Assert.NotNull(store.config.SyncObjects);
                Assert.Equal(store.config.ChunkCount, store.config.SyncObjects.Length);


                Assert.True((ulong)store.config.EndTablePointer - (ulong)store.config.TableFileBaseAddress <= (ulong)store.config.TableMemoryMapper.Length, "There is not enough space in the file for the table");
                Assert.True((store.config.TableMemoryMapper.fs.Length & Constants.AllocationGranularityMask) == 0, "The file length must be a multiple of AllocationGranulariry");

            }
            // we should be able to delete the file after disposing the store
            File.Delete(filePath);

        }

        [Fact]
        public void MediumTableShouldBeCorrectlyConfigured()
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            Func<long, long> hashFunction = key => key;

            int chunkCount = Math.Min(Environment.ProcessorCount * Environment.ProcessorCount * 16, 8192);
            if (Bits.IsPowerOfTwo(chunkCount) == false) chunkCount = Math.Min(Bits.NextPowerOf2(chunkCount), 8192);
            long chunkSize = 128;
            long slotCount = chunkCount * chunkSize;
            long capacity = slotCount - slotCount / 8;

            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, capacity, hashFunction))
            {
                store.EnsureInitialized();
                Assert.Equal(capacity, store.config.Capacity);
                Assert.Equal(7, store.config.ChunkBits);
                Assert.Equal(slotCount/chunkSize, store.config.ChunkCount);
                Assert.Equal(chunkSize -1, store.config.ChunkMask);
                Assert.Equal(chunkSize, store.config.ChunkSize);
                Assert.Equal(filePathWithoutExtension + ".DataFile", store.config.DataFilePath);
                Assert.Null(store.config.DataFile);
                Assert.Equal((ulong)(store.config.TablePointer + store.config.RecordSize * store.config.SlotCount), (ulong)store.config.EndTablePointer);
                Assert.Equal(hashFunction, store.config.HashFunction);
                Assert.Equal(slotCount - 1, store.config.HashMask);
                Assert.Equal(filePath, store.config.HashTableFilePath);

                Assert.Equal((ulong)store.config.TableFileBaseAddress, (ulong)store.config.HeaderPointer);
                Assert.True(store.config.IsNew);
                Assert.False(store.config.IsThreadSafe);
                Assert.Equal(store.config.KeyComparer, EqualityComparer<long>.Default);
                Assert.True(store.config.MaxAllowedDistance > 384 && store.config.MaxAllowedDistance < 4096);
                Assert.Equal((store.config.MaxAllowedDistance + store.config.ChunkSize -1)/ store.config.ChunkSize + 1, store.config.MaxLocksPerOperation);
                Assert.Equal(18, store.config.RecordSize);
                Assert.Equal(Bits.MostSignificantBit(store.config.SlotCount), store.config.SlotBits);
                Assert.Equal(slotCount, store.config.SlotCount);
                Assert.Null(store.config.SyncObjects);
                Assert.Equal((ulong)store.config.TableMappingSession.GetBaseAddress(), (ulong)store.config.TableFileBaseAddress);
                Assert.NotNull(store.config.TableMappingSession);
                Assert.NotNull(store.config.TableMemoryMapper);
                Assert.Equal((ulong)(store.config.TableFileBaseAddress + sizeof(StaticHashTableFileHeader)), (ulong)store.config.TablePointer);
                Assert.Equal(EqualityComparer<long>.Default, store.config.ValueComparer);

                Assert.Equal(store.Capacity, store.config.Capacity);
                Assert.Equal(0, store.Count);
                Assert.Equal(store.config.DataFilePath, store.DataFilePath);
                Assert.Equal(store.config.HashTableFilePath, store.HashTableFilePath);
                Assert.False(store.IsDisposed);
                Assert.Equal(0.0, store.LoadFactor);
                Assert.Equal(0, store.MaxDistance);
                Assert.Equal(float.NaN, store.MeanDistance);

                Assert.True(Bits.IsPowerOfTwo(store.config.ChunkSize), "ChunkSize must be a power of two");
                Assert.True(store.config.MaxLocksPerOperation > 1 && store.config.MaxLocksPerOperation <= 8, "MaxLocsPerOperation must be between 1 and 8");
                Assert.True((store.config.MaxLocksPerOperation - 1) * store.config.ChunkSize >= store.config.MaxAllowedDistance, "MaxAllowedDistance must be covered with MaxLocksPerOperation locks");
                Assert.True(store.config.MaxAllowedDistance <= (store.config.ChunkCount - 2) * store.config.ChunkSize, "MaxAllowedDistance must be reached before deadlocking.");


                store.config.IsThreadSafe = true;
                store.EnsureInitialized();
                Assert.NotNull(store.config.SyncObjects);
                Assert.Equal(store.config.ChunkCount, store.config.SyncObjects.Length);


                Assert.True((ulong)store.config.EndTablePointer - (ulong)store.config.TableFileBaseAddress <= (ulong)store.config.TableMemoryMapper.Length, "There is not enough space in the file for the table");
                Assert.True((store.config.TableMemoryMapper.fs.Length & Constants.AllocationGranularityMask) == 0, "The file length must be a multiple of AllocationGranulariry");

            }
            // we should be able to delete the file after disposing the store
            File.Delete(filePath);

        }

        [Fact]
        public void BigTableShouldBeCorrectlyConfigured()
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            Func<long, long> hashFunction = key => key;

            int chunkCount = Math.Min(Environment.ProcessorCount * Environment.ProcessorCount * 16, 8192);
            if (Bits.IsPowerOfTwo(chunkCount) == false) chunkCount = Math.Min(Bits.NextPowerOf2(chunkCount), 8192);
            long chunkSize = 16384;
            long slotCount = chunkCount * chunkSize;
            long capacity = slotCount - slotCount / 8;

            using (store = new StaticFixedSizeStore<long, long>(filePathWithoutExtension, capacity, hashFunction))
            {
                store.EnsureInitialized();
                Assert.Equal(capacity, store.config.Capacity);
                Assert.Equal(14, store.config.ChunkBits);
                Assert.Equal(slotCount / chunkSize, store.config.ChunkCount);
                Assert.Equal(chunkSize - 1, store.config.ChunkMask);
                Assert.Equal(chunkSize, store.config.ChunkSize);
                Assert.Equal(filePathWithoutExtension + ".DataFile", store.config.DataFilePath);
                Assert.Null(store.config.DataFile);
                Assert.Equal((ulong)(store.config.TablePointer + store.config.RecordSize * store.config.SlotCount), (ulong)store.config.EndTablePointer);
                Assert.Equal(hashFunction, store.config.HashFunction);
                Assert.Equal(slotCount - 1, store.config.HashMask);
                Assert.Equal(filePath, store.config.HashTableFilePath);

                Assert.Equal((ulong)store.config.TableFileBaseAddress, (ulong)store.config.HeaderPointer);
                Assert.True(store.config.IsNew);
                Assert.False(store.config.IsThreadSafe);
                Assert.Equal(store.config.KeyComparer, EqualityComparer<long>.Default);
                Assert.True(store.config.MaxAllowedDistance > 384 && store.config.MaxAllowedDistance < 4096);
                Assert.Equal(2, store.config.MaxLocksPerOperation);
                Assert.Equal(18, store.config.RecordSize);
                Assert.Equal(Bits.MostSignificantBit(store.config.SlotCount), store.config.SlotBits);
                Assert.Equal(slotCount, store.config.SlotCount);
                Assert.Null(store.config.SyncObjects);
                Assert.Equal((ulong)store.config.TableMappingSession.GetBaseAddress(), (ulong)store.config.TableFileBaseAddress);
                Assert.NotNull(store.config.TableMappingSession);
                Assert.NotNull(store.config.TableMemoryMapper);
                Assert.Equal((ulong)(store.config.TableFileBaseAddress + sizeof(StaticHashTableFileHeader)), (ulong)store.config.TablePointer);
                Assert.Equal(EqualityComparer<long>.Default, store.config.ValueComparer);

                Assert.Equal(store.Capacity, store.config.Capacity);
                Assert.Equal(0, store.Count);
                Assert.Equal(store.config.DataFilePath, store.DataFilePath);
                Assert.Equal(store.config.HashTableFilePath, store.HashTableFilePath);
                Assert.False(store.IsDisposed);
                Assert.Equal(0.0, store.LoadFactor);
                Assert.Equal(0, store.MaxDistance);
                Assert.Equal(float.NaN, store.MeanDistance);

                Assert.True(Bits.IsPowerOfTwo(store.config.ChunkSize), "ChunkSize must be a power of two");
                Assert.True(store.config.MaxLocksPerOperation > 1 && store.config.MaxLocksPerOperation <= 8, "MaxLocsPerOperation must be between 1 and 8");
                Assert.True((store.config.MaxLocksPerOperation - 1) * store.config.ChunkSize >= store.config.MaxAllowedDistance, "MaxAllowedDistance must be covered with MaxLocksPerOperation locks");
                Assert.True(store.config.MaxAllowedDistance <= (store.config.ChunkCount - 2) * store.config.ChunkSize, "MaxAllowedDistance must be reached before deadlocking.");


                store.config.IsThreadSafe = true;
                store.EnsureInitialized();
                Assert.NotNull(store.config.SyncObjects);
                Assert.Equal(store.config.ChunkCount, store.config.SyncObjects.Length);


                Assert.True((ulong)store.config.EndTablePointer - (ulong)store.config.TableFileBaseAddress <= (ulong)store.config.TableMemoryMapper.Length, "There is not enough space in the file for the table");
                Assert.True((store.config.TableMemoryMapper.fs.Length & Constants.AllocationGranularityMask) == 0, "The file length must be a multiple of AllocationGranulariry");

            }
            // we should be able to delete the file after disposing the store
            File.Delete(filePath);

        }



    }
}
