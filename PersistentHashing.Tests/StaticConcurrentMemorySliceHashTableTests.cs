using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using PersistentHashing;
using Xunit;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PersistentHashing.Tests
{
    public unsafe class Int32ArrayEqualityComparer : IEqualityComparer<int[]>
    {
        public bool Equals(int[] x, int[] y)
        {
            return x.AsSpan().SequenceEqual(y.AsSpan());
        }

        public int GetHashCode(int[] array)
        {
            fixed(int* pointer = array)
            {
                return unchecked((int) Hashing.FastHash32((byte*)pointer, (uint)(array.Length * sizeof(int))));
            }
        }
    }


    public unsafe class StaticConcurrentMemorySliceHashTableTests
    {


        private static readonly Int32ArrayEqualityComparer int32ArrayEqualityComparer = new Int32ArrayEqualityComparer();


        private static int[] CreateRandomArray(Random rnd)
        {
            var length = rnd.Next(10);
            var randomArray = new int[length];
            for (int i = 0; i < length; i++)
            {
                randomArray[i] = rnd.Next();
            }
            return randomArray;
        }

        private Dictionary<int[], int[]> CreateRandomDictionary(int n=56)
        {
            var dic = new Dictionary<int[], int[]>(int32ArrayEqualityComparer);
            var rnd = new Random(0);
            while (dic.Count < n)
            {
                dic.TryAdd(CreateRandomArray(rnd), CreateRandomArray(rnd));
            }
            return dic;
        }

        private Dictionary<long, long> CreateRandomIntDictionary(int n)
        {
            var dic = new Dictionary<long, long>();
            var rnd = new Random(0);
            while (dic.Count < n)
            {
                dic.TryAdd(rnd.Next(), rnd.Next());
            }
            return dic;
        }

        StaticStore store;
        StaticConcurrentHashTable hashTable;




        private StaticStore CreateStore(long capacity, Func<MemorySlice, long> hashFunction = null)
        {
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            var dataFilePath = filePathWithoutExtension + ".DataFile";
            if (File.Exists(filePath)) File.Delete(filePath);
            if (File.Exists(dataFilePath)) File.Delete(dataFilePath);
            return Factory.GetStaticStore(filePathWithoutExtension, capacity, hashFunction, new HashTableOptions<MemorySlice, MemorySlice>
            {
                DataFileSizeGrowthIncrement= Constants.AllocationGranularity,
                InitialDataFileSize = Constants.AllocationGranularity
            });
        }

        private StaticConcurrentHashTable CreateHashTable(long capacity = 56, Func<MemorySlice, long> hashFunction = null)
        {
            store = CreateStore(capacity, hashFunction);
            hashTable = store.GetConcurrentHashTable();
            return hashTable;
        }

        private void DeleteHashTable()
        {
            if (hashTable != null) hashTable.Dispose();
            if (store != null) store.Dispose();
            if (store != null && File.Exists(store.config.DataFilePath)) File.Delete(store.config.DataFilePath);
            if (store != null && File.Exists(store.config.HashTableFilePath)) File.Delete(store.config.HashTableFilePath);
        }



        [Fact]
        public void ReadWhatYourWrite()
        {
           
            try
            {
                var dic = CreateRandomDictionary(56);
                CreateHashTable();
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    Assert.True(hashTable.Get(kv.Key).SequenceEquals(kv.Value));
                    Assert.True(hashTable.ContainsKey(kv.Key));
                }
            }
            finally
            {
                DeleteHashTable();
            }
        }


        [Fact]
        public void ClearShouldWorkAsExpectd()
        {
            var dic = CreateRandomDictionary(56);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }
                hashTable.Clear();
                Assert.Equal(0, hashTable.Count);
                Assert.Empty(hashTable);
            }
        }

        [Fact]
        public void TryUpdateShouldWorkAsExpectd()
        {
            try
            {
                CreateHashTable();
                int seven = 7;
                int six = 6;
                var sevenMemorySlice = new MemorySlice(&seven, sizeof(int));
                var sixMemorySlice = new MemorySlice(&six, sizeof(int));
                Assert.False(hashTable.TryUpdate(sevenMemorySlice, sevenMemorySlice, sevenMemorySlice));
                hashTable.Add(sevenMemorySlice, sevenMemorySlice);
                Assert.False(hashTable.TryUpdate(sevenMemorySlice, sixMemorySlice, sixMemorySlice));
                Assert.True(hashTable.TryUpdate(sevenMemorySlice, sixMemorySlice, sevenMemorySlice));
                Assert.Equal(6,  *(int*)(hashTable[sevenMemorySlice].Pointer));
                Assert.Single(hashTable);
            }
            finally
            {
                DeleteHashTable();
            }
        }


        [Fact]
        public void CanEnumerateTheItemsYourWrite()
        {
            
            try
            {
                var dic = CreateRandomDictionary(56);
                CreateHashTable();
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                foreach (var kv in hashTable)
                {
                    Assert.True(kv.Value.SequenceEquals(dic[kv.Key.ToArray<int>()]));
                    dic.Remove(kv.Key.ToArray<int>());
                }
                Assert.Empty(dic);

            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void CanEnumerateTheKeysYourWrite()
        {
            try 
            {
                var dic = CreateRandomDictionary(56);
                CreateHashTable();
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                foreach (var k in hashTable.Keys)
                {
                    Assert.True(dic.ContainsKey(k.ToArray<int>()));
                    dic.Remove(k.ToArray<int>());
                }
                Assert.Empty(dic);
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void CanEnumerateTheValuesYourWrite()
        {
           
            try
            {
                var dic = CreateRandomDictionary(56);
                CreateHashTable();
                var valuesDic = new Dictionary<int[], int>(new Int32ArrayEqualityComparer());
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);

                    if (valuesDic.TryGetValue(kv.Value, out int valueCount))
                    {
                        valuesDic[kv.Value]++;
                    }
                    else
                    {
                        valuesDic.Add(kv.Value, 1);
                    }
                }

                foreach (var v in hashTable.Values)
                {
                    var va = v.ToArray<int>();
                    Assert.True(valuesDic.ContainsKey(va));
                    valuesDic[va]--;
                }
                foreach (var v in valuesDic.Values)
                {
                    Assert.Equal(0, v);
                }
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void ExcedingCapatityShouldThrow()
        {

            try
            {
                var dic = CreateRandomDictionary(100);
                CreateHashTable();
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.TryAdd(kv.Key, kv.Value);
                    }
                });
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void AddingExistingKeyShouldThrow()
        {
           try
            {
                CreateHashTable();
                var key = new int[] { 1, 2, 3 };
                var value = new int[] { 4, 5, 6 };
                hashTable.TryAdd(key, value);
                Assert.Throws<ArgumentException>(() => hashTable.Add(key, value));
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void TryAddingExistingKeyShouldFail()
        {
            try
            {
                CreateHashTable();
                var key = new int[] { 1, 2, 3 };
                var value = new int[] { 4, 5, 6 };
                hashTable.TryAdd(key, value);
                Assert.False(hashTable.TryAdd(key, value));
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void RemoveShouldWorkAsExpected()
        {
            
            try
            {
                var dic = CreateRandomDictionary(56);
                CreateHashTable();

                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                count = dic.Count;
                foreach (var kv in dic)
                {
                    MemorySlice value;
                    fixed (void* keyPointer = kv.Key)
                    {
                        Assert.True(hashTable.TryRemove(kv.Key , out value));
                    }
                    count--;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    Assert.True(value.SequenceEquals(kv.Value));
                    Assert.False(hashTable.Remove(kv.Key));
                }
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void ItWorksDespitePoorHashFunctionIfMaxDistanceIsNotReached()
        {
           
            try
            {
                var dic = CreateRandomDictionary(45);
                CreateHashTable(56, key => key.Size == 0 ? 0 : *(byte*)key.Pointer & 1);

                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.True(hashTable.Get(kv.Key).SequenceEquals(kv.Value));
                }
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void PoorHashFunctionMightCauseMaxDistanceReached()
        {
            try
            {
                var dic = CreateRandomDictionary(100);
                CreateHashTable(56, key => key.Size == 0 ? 0 : *(byte*)key.Pointer & 1);
                long count = 0;
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.TryAdd(kv.Key, kv.Value);
                        count++;
                        Assert.Equal(count, hashTable.Count);
                        Assert.True(hashTable.Get(kv.Key).SequenceEquals(kv.Value));
                    }
                });
            }
            finally
            {
                DeleteHashTable();
            }
        }


        [Fact]
        public void GetOrAddValueFactoryIsCalledOnyWhenAdded()
        {

            try
            {
                CreateHashTable();
                int callCount = 0;
                int addedCount = 0;
                bool start = false;
                Guid addedGuid = Guid.Empty;
                var seven = 7;
                var sevenSlice = new MemorySlice(&seven, sizeof(int));
                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid();
                    var memorySliceToAdd = new MemorySlice(&guidToAdd, sizeof(Guid));
                    Func<MemorySlice, MemorySlice> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return memorySliceToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var existingOrAdded = hashTable.GetOrAdd(sevenSlice, valueFactory);
                    if ( existingOrAdded.SequenceEquals(memorySliceToAdd))
                    {
                        Interlocked.Increment(ref addedCount);
                        addedGuid = guidToAdd;
                    }
                };

                var tasks = new Task[8];
                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Factory.StartNew(action, TaskCreationOptions.LongRunning);
                }
                Thread.Sleep(100);
                start = true;
                Task.WaitAll(tasks);
                Assert.Equal(1, callCount);
                Assert.Equal(1, addedCount);
                Assert.Equal(1, hashTable.Count);
                Assert.Single(hashTable);
                Assert.True(hashTable.ContainsKey(sevenSlice));

                Guid addedGuidCopy = addedGuid;
                Assert.True(hashTable[sevenSlice].SequenceEquals(new MemorySlice(&addedGuidCopy, sizeof(Guid))));
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void TryAddValueFactoryIsCalledOnyWhenAdded()
        {
            
            try
            {
                CreateHashTable();
                int callCount = 0;
                int addedCount = 0;
                bool start = false;
                Guid addedGuid = Guid.Empty;

                var seven = 7;
                var sevenSlice = new MemorySlice(&seven, sizeof(int));

                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid();
                    var memorySliceToAdd = new MemorySlice(&guidToAdd, sizeof(Guid));
                    Func<MemorySlice, MemorySlice> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return memorySliceToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var added = hashTable.TryAdd(sevenSlice, valueFactory);
                    if (added)
                    {
                        Interlocked.Increment(ref addedCount);
                        addedGuid = guidToAdd;
                    }
                };

                var tasks = new Task[8];
                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Factory.StartNew(action, TaskCreationOptions.LongRunning);
                }
                Thread.Sleep(100);
                start = true;
                Task.WaitAll(tasks);
                Assert.Equal(1, callCount);
                Assert.Equal(1, addedCount);
                Assert.Equal(1, hashTable.Count);
                Assert.Single(hashTable);
                Assert.True(hashTable.ContainsKey(sevenSlice));

                Guid addedGuidCopy = addedGuid;
                Assert.True(hashTable[sevenSlice].SequenceEquals(new MemorySlice(&addedGuidCopy, sizeof(Guid))));
            }
            finally
            {
                DeleteHashTable();
            }
        }


        [Fact]
        public void AddOrUpdateShouldWorkAsExpected()
        {
           
           try
            {
                CreateHashTable();
                int callCount = 0;
                int addedCount = 0;
                bool start = false;
                var seven = 7;
                var sevenSlice = new MemorySlice(&seven, sizeof(int));

                Action action = () =>
                {
                    long valueToReturn;
                    var memorySliceToReturn = new MemorySlice(&valueToReturn, sizeof(long));

                    MemorySlice updateValueFactory(MemorySlice key, MemorySlice value)
                    {
                        Interlocked.Increment(ref callCount);
                        *(long*)memorySliceToReturn.Pointer = *(long*)value.Pointer + 1;
                        return memorySliceToReturn;
                    }
                    while (Volatile.Read(ref start) == false);

                    long addValue = 0;
                    var memorySliceToAdd = new MemorySlice(&addValue, sizeof(long));
                    var newValue = hashTable.AddOrUpdate(sevenSlice, memorySliceToAdd, updateValueFactory);
                    if (*(long*)newValue.Pointer == 0)
                    {
                        Interlocked.Increment(ref addedCount);
                    }
                };

                var tasks = new Task[8];
                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Factory.StartNew(action, TaskCreationOptions.LongRunning);
                }
                Thread.Sleep(100);
                start = true;
                Task.WaitAll(tasks);
                Assert.Equal(7, callCount);
                Assert.Equal(1, addedCount);
                Assert.Equal(1, hashTable.Count);
                Assert.Single(hashTable);
                Assert.True(hashTable.ContainsKey(sevenSlice));
                Assert.Equal(7, *(long *)hashTable[sevenSlice].Pointer);
            }
            finally
            {
                DeleteHashTable();
            }
        }

    }
}
