using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using PersistentHashing;
using Xunit;
using System.Linq;

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

        //private StaticConcurrentFixedSizeHashTable<long, long> hashTable;

        private StaticConcurrentFixedSizeHashTable<long, long> CreateHashTable(long capacity, Func<long, long> hashFunction = null)
        {
            hashFunction = hashFunction ?? new Func<long, long>(key => key);
            var filePathWithoutExtension = Guid.NewGuid().ToString("N");
            var filePath = filePathWithoutExtension + ".HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            return Factory.GetStaticConcurrentFixedSizeHashTable<long, long>(filePathWithoutExtension, capacity, hashFunction);
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
                    Assert.Equal(kv.Value, hashTable[kv.Key]);
                }
                hashTable.Dispose();
                File.Delete(hashTable.config.HashTableFilePath);
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
            StaticConcurrentFixedSizeHashTable<long, long> hashTable;
            var dic = CreateRandomDictionary(100);
            using (hashTable = CreateHashTable(56))
            {
               Assert.Throws<InvalidOperationException>(() =>
               {
                   foreach (var kv in dic)
                   {
                       hashTable.Add(kv);
                   }
               });
            }
            File.Delete(hashTable.config.HashTableFilePath);
        }
    }
}
