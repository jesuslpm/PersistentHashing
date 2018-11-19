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
    public unsafe class StaticConcurrentFixedKeySizeHashTableTests
    {

        private Dictionary<long, int[]> CreateRandomDictionary(int n)
        {
            var dic = new Dictionary<long, int[]>();
            var rnd = new Random(0);
            while (dic.Count < n)
            {
                var length = rnd.Next(10);
                var v = new int[length];
                for (int i = 0; i < length; i++)
                {
                    v[i] = rnd.Next();
                }
                dic.TryAdd(rnd.Next(), v);
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



        //private StaticConcurrentFixedSizeHashTable<long, long> hashTable;

        private StaticFixedKeySizeStore<long> CreateStore(long capacity, Func<long, long> hashFunction = null)
        {
            hashFunction = hashFunction ?? new Func<long, long>(key => key);
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            var dataFilePath = filePathWithoutExtension + ".DataFile";
            if (File.Exists(filePath)) File.Delete(filePath);
            if (File.Exists(dataFilePath)) File.Delete(dataFilePath);
            return Factory.GetStaticFixedKeySizeStore<long>(filePathWithoutExtension, capacity, hashFunction);
        }


        [Fact]
        public void ReadWhatYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    Assert.True(hashTable[kv.Key].SequenceEquals(kv.Value));
                    Assert.True(hashTable.ContainsKey(kv.Key));
                }
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
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
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void TryUpdateShouldWorkAsExpectd()
        {
            var dic = CreateRandomDictionary(56);
            int seven = 7;
            int six = 6;
            var sevenMemorySlice = new MemorySlice(&seven, sizeof(int));
            var sixMemorySlice = new MemorySlice(&six, sizeof(int));
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                Assert.False(hashTable.TryUpdate(7, sevenMemorySlice, sevenMemorySlice));
                hashTable.Add(7, sevenMemorySlice);
                Assert.False(hashTable.TryUpdate(7, sixMemorySlice, sixMemorySlice));
                Assert.True(hashTable.TryUpdate(7, sixMemorySlice, sevenMemorySlice));
                Assert.Equal(6,  *(int*)(hashTable[7].Pointer));
                Assert.Single(hashTable);
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }




        [Fact]
        public void CanEnumerateTheItemsYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                foreach (var kv in hashTable)
                {
                    Assert.True(kv.Value.SequenceEquals(dic[kv.Key]));
                    dic.Remove(kv.Key);
                }
                Assert.Empty(dic);
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void CanEnumerateTheKeysYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                foreach (var k in hashTable.Keys)
                {
                    Assert.True(dic.ContainsKey(k));
                    dic.Remove(k);
                }
                Assert.Empty(dic);
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void CanEnumerateTheValuesYourWrite()
        {
            var dic = CreateRandomIntDictionary(56);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                var valuesDic = new Dictionary<long, int>();
                foreach (var kv in dic)
                {
                    long v = kv.Value;
                    hashTable.TryAdd(kv.Key, new MemorySlice(&v, sizeof(long)));

                    if (valuesDic.TryGetValue(kv.Value, out int valueCount))
                    {
                        valuesDic[kv.Value]++;
                    }
                    else
                    {
                        valuesDic.Add(kv.Value, 1);
                    }
                }

                foreach (var v in hashTable.Values.Select(x => *(long*)x.Pointer))
                {
                    Assert.True(valuesDic.ContainsKey(v));
                    valuesDic[v]--;
                }
                foreach (var v in valuesDic.Values)
                {
                    Assert.Equal(0, v);
                }

                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void ExcedingCapatityShouldThrow()
        {
            var dic = CreateRandomDictionary(100);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.TryAdd(kv.Key, kv.Value);
                    }
                });
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);

            }
        }

        [Fact]
        public void AddingExistingKeyShouldThrow()
        {
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                int value = 2;
                var memorySlize = new MemorySlice(&value, sizeof(int));
                hashTable.TryAdd(8, memorySlize);
                Assert.Throws<ArgumentException>(() => hashTable.Add(8, memorySlize));

                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void TryAddingExistingKeyShouldFail()
        {
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                int value = 2;
                var memorySlize = new MemorySlice(&value, sizeof(int));
                hashTable.Add(8, memorySlize);
                Assert.False(hashTable.TryAdd(8, memorySlize));

                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void RemoveShouldWorkAsExpected()
        {
            var dic = CreateRandomDictionary(56);
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                count = dic.Count;
                foreach (var kv in dic)
                {
                    Assert.True(hashTable.TryRemove(kv.Key, out var value));
                    count--;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    Assert.True(value.SequenceEquals(kv.Value));
                    Assert.False(hashTable.Remove(kv.Key));
                }

                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void ItWorksDespitePoorHashFunctionIfMaxDistanceIsNotReached()
        {
            var dic = CreateRandomDictionary(45);
            using (var store = CreateStore(56, key => key & 1))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.True(hashTable[kv.Key].SequenceEquals(kv.Value));
                }
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void PoorHashFunctionMightCauseMaxDistanceReached()
        {
            var dic = CreateRandomDictionary(56);
            using (var store = CreateStore(56, key => key & 1))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                long count = 0;
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.TryAdd(kv.Key, kv.Value);
                        count++;
                        Assert.Equal(count, hashTable.Count);
                        Assert.True(hashTable[kv.Key].SequenceEquals(kv.Value));
                    }
                });
                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }


        [Fact]
        public void GetOrAddValueFactoryIsCalledOnyWhenAdded()
        {
            int callCount = 0;
            int addedCount = 0;
            bool start = false;
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {
                Guid addedGuid = Guid.Empty;

                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid();
                    var memorySliceToAdd = new MemorySlice(&guidToAdd, sizeof(Guid));
                    Func<long, MemorySlice> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return memorySliceToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var existingOrAdded = hashTable.GetOrAdd(7, valueFactory);
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
                Assert.True(hashTable.ContainsKey(7));

                Guid addedGuidCopy = addedGuid;
                Assert.True(hashTable[7].SequenceEquals(new MemorySlice(&addedGuidCopy, sizeof(Guid))));

                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }

        [Fact]
        public void TryAddValueFactoryIsCalledOnyWhenAdded()
        {
            int callCount = 0;
            int addedCount = 0;
            bool start = false;
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {

                Guid addedGuid = Guid.Empty;

                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid();
                    var memorySliceToAdd = new MemorySlice(&guidToAdd, sizeof(Guid));
                    Func<long, MemorySlice> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return memorySliceToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var added = hashTable.TryAdd(7, valueFactory);
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
                Assert.True(hashTable.ContainsKey(7));

                Guid addedGuidCopy = addedGuid;
                Assert.True(hashTable[7].SequenceEquals(new MemorySlice(&addedGuidCopy, sizeof(Guid))));

                hashTable.Dispose();
                store.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
                File.Delete(hashTable.config.DataFilePath);
            }
        }


        [Fact]
        public void AddOrUpdateShouldWorkAsExpected()
        {
            int callCount = 0;
            int addedCount = 0;
            bool start = false;
            using (var store = CreateStore(56))
            using (var hashTable = store.GetConcurrentHashTable())
            {

                Action action = () =>
                {
                    long valueToReturn;
                    var memorySliceToReturn = new MemorySlice(&valueToReturn, sizeof(long));

                    MemorySlice updateValueFactory(long key, MemorySlice value)
                    {
                        Interlocked.Increment(ref callCount);
                        *(long*)memorySliceToReturn.Pointer = *(long*)value.Pointer + 1;
                        return memorySliceToReturn;
                    }
                    while (Volatile.Read(ref start) == false);

                    long addValue = 0;
                    var memorySliceToAdd = new MemorySlice(&addValue, sizeof(long));
                    var newValue = hashTable.AddOrUpdate(7, memorySliceToAdd, updateValueFactory);
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
                Assert.True(hashTable.ContainsKey(7));
                Assert.Equal(7, *(long *)( hashTable[7].Pointer));
            }
        }

    }
}
