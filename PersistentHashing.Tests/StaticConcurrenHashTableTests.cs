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
    public unsafe class StaticConcurrentHashTableTests
    {

        private Dictionary<string, string> CreateRandomDictionary(int n)
        {
            var dic = new Dictionary<string, string>();
            var rnd = new Random(0);
            while (dic.Count < n)
            {
                dic.TryAdd(rnd.Next().ToString(), rnd.Next().ToString());
            }
            return dic;
        }


        StaticStore<string, string> store;
        StaticConcurrentHashTable<string, string> hashTable;


        private StaticStore<string, string> CreateStore(long capacity, Func<string, long> hashFunction = null)
        {
            hashFunction = hashFunction ?? Hashing.FastHash64;
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            var dataFilePath = filePathWithoutExtension + ".DataFile";
            if (File.Exists(filePath)) File.Delete(filePath);
            if (File.Exists(dataFilePath)) File.Delete(dataFilePath);
            var serializer = new StringStringSerializer();
            return Factory.GetStaticStore<string, string>(filePathWithoutExtension, capacity, hashFunction, serializer, new HashTableOptions<string, string>
            {
                DataFileSizeGrowthIncrement = Constants.AllocationGranularity,
                InitialDataFileSize = Constants.AllocationGranularity,
                KeyComparer = StringComparer.Ordinal,
                ValueComparer = StringComparer.Ordinal
            });
        }

        private StaticConcurrentHashTable<string, string> CreateHashTable(long capacity = 56, Func<string, long> hashFunction = null)
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
                var dic = CreateRandomDictionary(54);
                dic.Add("null", null);
                CreateHashTable(56);
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    var value = hashTable[kv.Key];
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
                    Assert.True(hashTable.ContainsKey(kv.Key));
                }
                // dictionary doesn't support null keys, but HashTable does.
                hashTable.Add(null, null);
                Assert.Null(hashTable[null]);
            }
            finally
            {
                DeleteHashTable();
            }

        }


        [Fact]
        public void ClearShouldWorkAsExpectd()
        {
            try
            {
                var dic = CreateRandomDictionary(56);
                CreateHashTable(56);
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }
                hashTable.Clear();
                Assert.Equal(0, hashTable.Count);
                Assert.Empty(hashTable);
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void TryUpdateShouldWorkAsExpectd()
        {
            try
            {
                CreateHashTable(56);
                var seven = "seven";
                var six = "six";
                Assert.False(hashTable.TryUpdate(seven, seven, seven));
                hashTable.Add(seven, seven);
                Assert.False(hashTable.TryUpdate(seven, six, six));
                Assert.True(hashTable.TryUpdate(seven, six, seven));
                Assert.Equal(six, hashTable[seven]);
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
                CreateHashTable(56);

                foreach (var kv in dic)
                {
                    hashTable.Add(kv.Key, kv.Value);
                }

                foreach (var kv in hashTable)
                {
                    Assert.Equal(dic[kv.Key], kv.Value);
                    dic.Remove(kv.Key);
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
                CreateHashTable(56);
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
            }
            finally
            {
                DeleteHashTable();
            }
        }

        [Fact]
        public void ItemsShouldBeOrderedByCurrentPortionOfHashValue()
        {
            try
            {
                var dic = CreateRandomDictionary(1792);
                CreateHashTable(1792);
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                }

                long previousHashPortion = -1;
                int i = 0;
                foreach (var k in hashTable.Keys)
                {
                    if (i++ > 10)
                    {
                        long hashPortion = hashTable.config.HashFunction(k) & hashTable.config.HashMask;
                        Assert.True(previousHashPortion <= hashPortion);
                        previousHashPortion = hashPortion;
                    }
                }
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
                var dic = CreateRandomDictionary(56); ;
                CreateHashTable(56);
                var valuesDic = new Dictionary<string, int>();
                foreach (var kv in dic)
                {
                    hashTable.Add(kv.Key, kv.Value);
                    if (valuesDic.ContainsKey(kv.Value))
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
                    Assert.True(valuesDic.ContainsKey(v));
                    valuesDic[v]--;
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
                CreateHashTable(56);
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
                CreateHashTable(56);
                hashTable.TryAdd("8", "someValue");
                Assert.Throws<ArgumentException>(() => hashTable.Add("8", "someValue"));
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
                CreateHashTable(56);
                hashTable.Add("8", "someValue");
                Assert.False(hashTable.TryAdd("8", "someValue"));
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
                CreateHashTable(56);
                var dic = CreateRandomDictionary(56);
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
                    Assert.Equal(value, kv.Value);
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
                CreateHashTable(56, key => key.Length & 1);
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.TryAdd(kv.Key, kv.Value);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
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
                var dic = CreateRandomDictionary(56);
                CreateHashTable(56, key => key.Length & 1);
                long count = 0;
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.TryAdd(kv.Key, kv.Value);
                        count++;
                        Assert.Equal(count, hashTable.Count);
                        Assert.Equal(kv.Value, hashTable[kv.Key]);
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
                string addedGuid = null;

                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid().ToString("N");
                    Func<string, string> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return guidToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var existingOrAdded = hashTable.GetOrAdd("7", valueFactory);
                    if ( existingOrAdded==guidToAdd)
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
                Assert.True(hashTable.ContainsKey("7"));
                Assert.Equal(addedGuid, hashTable["7"]);
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
                int callCount = 0;
                int addedCount = 0;
                bool start = false;
                CreateHashTable();

                string addedGuid = null;

                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid().ToString("N");
                    Func<string, string> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return guidToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var added = hashTable.TryAdd("7", valueFactory);
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
                Assert.True(hashTable.ContainsKey("7"));

                Assert.Equal(addedGuid, hashTable["7"]);
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

                Action action = () =>
                {
                    string updateValueFactory(string key, string value)
                    {
                        Interlocked.Increment(ref callCount);
                        return (long.Parse(value) + 1).ToString();
                    }
                    while (Volatile.Read(ref start) == false);

                    var newValue = hashTable.AddOrUpdate("7", "0", updateValueFactory);
                    if (newValue == "0")
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
                Assert.True(hashTable.ContainsKey("7"));
                Assert.Equal("7", hashTable["7"]);
            }
            finally
            {
                DeleteHashTable();
            }
        }

    }
}
