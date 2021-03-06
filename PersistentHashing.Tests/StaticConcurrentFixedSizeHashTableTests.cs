﻿using System;
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
    public class StaticConcurrentFixedSizeHashTableTests
    {

        private Dictionary<long, long> CreateRandomDictionary(int n)
        {
            var dic = new Dictionary<long, long>();
            var rnd = new Random(0);
            while (dic.Count < n)
            {
                dic.TryAdd(rnd.Next(), rnd.Next());
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

        private StaticConcurrentFixedSizeHashTable<long, long> CreateHashTable(long capacity, Func<long, long> hashFunction = null)
        {
            hashFunction = hashFunction ?? new Func<long, long>(key => key);
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            return Factory.GetStaticConcurrentFixedSizeHashTable<long, long>(filePathWithoutExtension, capacity, hashFunction);
        }


        private StaticConcurrentFixedSizeHashTable<long, Guid> CreateGuidHashTable(long capacity, Func<long, long> hashFunction = null)
        {
            hashFunction = hashFunction ?? new Func<long, long>(key => key);
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            return Factory.GetStaticConcurrentFixedSizeHashTable<long, Guid>(filePathWithoutExtension, capacity, hashFunction);
        }

        [Fact]
        public void ReadWhatYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
                    Assert.True(hashTable.ContainsKey(kv.Key));
                }
                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }


        [Fact]
        public void ClearShouldWorkAsExpectd()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                }
                hashTable.Clear();
                Assert.Equal(0, hashTable.Count);
                Assert.Empty(hashTable);
                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }

        [Fact]
        public void TryUpdateShouldWorkAsExpectd()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                Assert.False(hashTable.TryUpdate(7, 7, 7));
                hashTable.Add(7, 7);
                Assert.False(hashTable.TryUpdate(7, 6, 6));
                Assert.True(hashTable.TryUpdate(7, 6, 7));
                Assert.Equal(6, hashTable[7]);
                Assert.Single(hashTable);
            }
        }




        [Fact]
        public void CanEnumerateTheItemsYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
                }

                foreach (var kv in hashTable)
                {
                    Assert.Equal(dic[kv.Key], kv.Value);
                    dic.Remove(kv.Key);
                }
                Assert.Empty(dic);

                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }

        [Fact]
        public void CanEnumerateTheKeysYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
                }

                foreach (var k in hashTable.Keys)
                {
                    Assert.True(dic.ContainsKey(k));
                    dic.Remove(k);
                }
                Assert.Empty(dic);

                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }

        [Fact]
        public void CanEnumerateTheValuesYourWrite()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                long count = 0;
                var valuesDic = new Dictionary<long, int>();
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(kv.Value, hashTable[kv.Key]);

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
                    Assert.True(valuesDic.ContainsKey(v));
                    valuesDic[v]--;
                }
                foreach (var v in valuesDic.Values)
                {
                    Assert.Equal(0, v);
                }

                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }

        [Fact]
        public void ExcedingCapatityShouldThrow()
        {
            var dic = CreateRandomDictionary(100);
            using (var hashTable = CreateHashTable(56))
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.Add(kv);
                    }
                });
                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);

            }
        }

        [Fact]
        public void AddingExistingKeyShouldThrow()
        {
            using (var hasTable = CreateHashTable(56))
            {
                hasTable.Add(2, 2);
                Assert.Throws<ArgumentException>(() => hasTable.Add(2, 1));
            }
        }

        [Fact]
        public void TryAddingExistingKeyShouldFail()
        {
            using (var hasTable = CreateHashTable(56))
            {
                hasTable.Add(2, 2);
                Assert.False(hasTable.TryAdd(2, 1));
            }
        }

        [Fact]
        public void RemoveShouldWorkAsExpected()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56))
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                }

                count = dic.Count;
                foreach (var kv in dic)
                {
                    Assert.True(hashTable.TryRemove(kv.Key, out long value));
                    count--;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(count, hashTable.Count());
                    Assert.Equal(kv.Value, value);
                    Assert.False(hashTable.Remove(kv.Key));
                }
            }
        }

        [Fact]
        public void ItWorksDespitePoorHashFunctionIfMaxDistanceIsNotReached()
        {
            var dic = CreateRandomDictionary(45);
            using (var hashTable = CreateHashTable(56, x => x & 1))
            {
                long count = 0;
                foreach (var kv in dic)
                {
                    hashTable.Add(kv);
                    count++;
                    Assert.Equal(count, hashTable.Count);
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
                }
                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }

        [Fact]
        public void PoorHashFunctionMightCauseMaxDistanceReached()
        {
            var dic = CreateRandomDictionary(56);
            using (var hashTable = CreateHashTable(56, x => x & 1))
            {
                long count = 0;
                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kv in dic)
                    {
                        hashTable.Add(kv);
                        count++;
                        Assert.Equal(count, hashTable.Count);
                        Assert.Equal(kv.Value, hashTable[kv.Key]);
                    }
                });
                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
            }
        }


        [Fact]
        public void GetOrAddValueFactoryIsCalledOnyWhenAdded()
        {
            int callCount = 0;
            int addedCount = 0;
            bool start = false;
            using (var hashTable = CreateGuidHashTable(56))
            {
                Guid addedGuid = Guid.Empty;
                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid();
                    Func<long, Guid> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return guidToAdd;
                    };
                    while (Volatile.Read(ref start) == false) ;
                    var existingOrAdded = hashTable.GetOrAdd(7, valueFactory);
                    if (existingOrAdded == guidToAdd)
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
                Assert.Equal(addedGuid, hashTable[7]);
            }
        }

        [Fact]
        public void TryAddValueFactoryIsCalledOnyWhenAdded()
        {
            int callCount = 0;
            int addedCount = 0;
            bool start = false;
            using (var hashTable = CreateGuidHashTable(56))
            {

                Guid addedGuid = Guid.Empty;

                Action action = () =>
                {
                    var guidToAdd = Guid.NewGuid();
                    Func<long, Guid> valueFactory = (key) =>
                    {
                        Interlocked.Increment(ref callCount);
                        return guidToAdd;
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
                Assert.Equal(addedGuid, hashTable[7]);
            }
        }


        [Fact]
        public void AddOrUpdateShouldWorkAsExpected()
        {
            int callCount = 0;
            int addedCount = 0;
            bool start = false;
            using (var hashTable = CreateHashTable(56))
            {

                Action action = () =>
                {
                    long updateValueFactory(long key, long value)
                    {
                        Interlocked.Increment(ref callCount);
                        return value + 1;
                    }
                    while (Volatile.Read(ref start) == false) ;
                    var newValue = hashTable.AddOrUpdate(7, 0, updateValueFactory);
                    if (newValue == 0)
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
                Assert.Equal(7, hashTable[7]);
            }
        }

    }
}
