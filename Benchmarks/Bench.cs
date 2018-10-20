using PersistentHashing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Benchmarks
{
    static class Bench
    {
        const int n = 10_000_000;

        public static void BenchMark()
        {
            Func<IEnumerable<string>>[] benchmarFunctions = new Func<IEnumerable<string>>[]
            {
                BenchmarkDictionarySequential, BenchmarkDictionaryRandom, BenchmarkHashTableSequential, BenchmarkHashTableRandom,
                BenchmarkLock, BenchmarkReaderWriterLockSlim, BenchmarkReaderWriterLock, BenchmarkSpinLock
            };

            // warm up
            //for (int i = 0; i < benchmarFunctions.Length; i++)
            //{
            //    benchmarFunctions[i]().ToList();
            //}

            // show results
            for (int i = 0; i < benchmarFunctions.Length; i++)
            {
                foreach (var line in benchmarFunctions[i]())
                {
                    Console.WriteLine(line);
                }
                Console.WriteLine();
            }
        }

        static volatile bool stop = false;
        static long reads = 0;
        static long writes = 0;
        static SynchonizedOperation operation = new SynchonizedOperation();

        static void SynchonizedOperationTest()
        {
            var operation = new SynchonizedOperation();
            var writeTask = Task.Run((Action)Write);
            var readTask = Task.Run((Action)Read);
            Task.Delay(TimeSpan.FromSeconds(5)).ContinueWith(_ => { stop = true; }).Wait();
            readTask.Wait(); writeTask.Wait();

            Console.WriteLine($"reads: {reads}, writes: {writes}");
        }

        static void Read()
        {
            Func<int> func = () => 1;
            while (!stop)
            {
                operation.Read(func);
                reads++;
            }
        }

        static void Write()
        {
            Func<int> func = () => 1;
            while (!stop)
            {
                operation.Write(func);
                writes++;
            }
        }

        static string BenchmarkAction(string description, Action<int> action, int times = n)
        {
            var watch = Stopwatch.StartNew();
            for (int i = 0; i < times; i++) action(i);
            watch.Stop();
            return description + $" in {watch.Elapsed}";
        }


        static IEnumerable<string> BenchmarkDictionarySequential()
        {
            var dic = new Dictionary<int, int>(n);
            yield return "Dictionary sequential access";
            yield return BenchmarkAction($"Added {n:0,0} items to Dictionary", (i) => dic.Add(i, i));
        }



        static IEnumerable<string> BenchmarkDictionaryRandom()
        {
            var dic = new Dictionary<long, long>(n);
            var rnd = new Random(0);
            yield return "Dictionary random access";
            yield return BenchmarkAction($"Added {n:0,0} items to Dictionary", (i) =>
            {
                long x;
                do
                {
                    x = rnd.Next();
                } while (dic.ContainsKey(x));
                dic.Add(x, x);
            });

        }

        static IEnumerable<string> BenchmarkHashTableRandom()
        {
            //return BenchmarkHashTable((key) => (ulong)(key), (hashTable, i) => hashTable.Add(i, i));
            string filePath = "Int64Int64.hash-table";
            if (File.Exists(filePath)) File.Delete(filePath);

            var rnd = new Random(0);
            using (var hashTable = new FixedSizeHashTable<long, long>(filePath, n, key => key, null, false))
            {
                yield return "FixedSizeHashTable random access benchmark";
                yield return BenchmarkAction($"Added {n:0,0} items to HashTable", (i) =>
                {
                    long x;
                    do
                    {
                        x = rnd.Next();
                    } while (hashTable.ContainsKey(x));
                    hashTable.Add(x, x);
                });
                yield return $"HashTable MaxDistance:  { hashTable.MaxDistance}";
                yield return BenchmarkAction("HashTable flushed", _ => hashTable.Flush(), 1);
                rnd = new Random(0);
                yield return BenchmarkAction($"Read {n:0,0} items from HashTable", i =>
                {
                    long key = rnd.Next();
                    hashTable.TryGet(key, out long value);
                    if (value != key) throw new InvalidOperationException("Test failed");
                });
            }
        }

        static IEnumerable<string> BenchmarkHashTableSequential()
        {
            //return BenchmarkHashTable((key) => (ulong)(key), (hashTable, i) => hashTable.Add(i, i));
            string filePath = "Int64Int64.hash-table";
            if (File.Exists(filePath)) File.Delete(filePath);
            using (var hashTable = new FixedSizeHashTable<long, long>(filePath, n, key => key, null, false))
            {
                yield return "FixedSizeHashTable sequencial access benchmark";
                yield return BenchmarkAction($"Added {n:0,0} items to HashTable", (i) => hashTable.Add(i, i));
                yield return $"HashTable MaxDistance:  { hashTable.MaxDistance}";
                yield return BenchmarkAction("HashTable flushed", _ => hashTable.Flush(), 1);
                yield return BenchmarkAction($"Read {n:0,0} items from HashTable", i =>
                {
                    hashTable.TryGet(i, out long v);
                    if (v != i) throw new InvalidOperationException("Test failed");
                });
            }
        }


        static IEnumerable<string> BenchmarkHashTable(Func<long, long> hashFunction, Action<FixedSizeHashTable<long, long>, int> addAction)
        {
            string filePath = "Int64Int64.hash-table";
            if (File.Exists(filePath)) File.Delete(filePath);
            using (var hashTable = new FixedSizeHashTable<long, long>(filePath, n, hashFunction, null, false))
            {
                yield return BenchmarkAction($"Added {n:0,0} items to HashTable", (i) => addAction(hashTable, i));
                yield return $"HashTable MaxDistance:  { hashTable.MaxDistance}";
                yield return BenchmarkAction("HashTable flushed", _ => hashTable.Flush(), 1);
                yield return BenchmarkAction($"Read {n:0,0} items from HashTable", i =>
                {
                    hashTable.TryGet(i, out long v);
                    if (v != i) throw new InvalidOperationException("Test failed");
                });
            }
        }

        static IEnumerable<string> BenchmarkLock()
        {
            object syncObject = new object();
            yield return "Lock no contention";
            yield return BenchmarkAction($"Executed Monitor.Enter and Monitor.Exit {n:0,0} times", _ =>
            {
                lock (syncObject) { }
            });
        }

        static IEnumerable<string> BenchmarkReaderWriterLockSlim()
        {
            ReaderWriterLockSlim readerWriterLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            yield return "ReaderWriterLockSlim NoRecursion no contention";
            yield return BenchmarkAction($"Executed EnterReadLock and ExitReadLock {n:0,0} times", _ =>
            {
                readerWriterLock.EnterReadLock();
                readerWriterLock.ExitReadLock();
            });
            yield return BenchmarkAction($"Executed EnterWriteLock and ExitWriteLock {n:0,0} times", _ =>
            {
                readerWriterLock.EnterWriteLock();
                readerWriterLock.ExitWriteLock();
            });
        }

        static IEnumerable<string> BenchmarkReaderWriterLock()
        {
            ReaderWriterLock readerWriterLock = new ReaderWriterLock();
            yield return "ReaderWriterLock no contention";
            yield return BenchmarkAction($"Executed AcquireReaderLock and ReleaseReaderLock {n:0,0} times", _ =>
            {
                readerWriterLock.AcquireReaderLock(10000);
                readerWriterLock.ReleaseReaderLock();
            });
            yield return BenchmarkAction($"Executed AcquireWriterLock and ReleaseWriterLock {n:0,0} times", _ =>
            {
                readerWriterLock.AcquireWriterLock(10000);
                readerWriterLock.ReleaseWriterLock();
            });
        }

        static IEnumerable<string> BenchmarkSpinLock()
        {
            SpinLock spinLock = new SpinLock();
            yield return "SpinLock no contention";
            yield return BenchmarkAction($"Executed Enter and Exit {n:0,0} times", _ =>
            {
                bool lockTaken = false;
                spinLock.Enter(ref lockTaken);
                if (lockTaken) spinLock.Exit();
            });
        }
    }
}
