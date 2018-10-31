using PersistentHashing;
using System;
using System.Collections.Concurrent;
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
        const int n = 100_000_000;

        private static readonly int ThreadCount = Environment.ProcessorCount;

        public static void BenchMark()
        {
            Func<IEnumerable<string>>[] benchmarFunctions = new Func<IEnumerable<string>>[]
            {
                //BenchmarkDictionarySequential, BenchmarkDictionaryRandom,
                //BenchmarkStaticFixedSizeHashTableSequential, BenchmarkStaticFixedSizeHashTableRandom,
                //BenchmarkStaticFixedSizeHashTableMultiThreaded,
                BenchmarkConcurrentDictionaryMultiThreaded,
                //BenchmarkReaderWriterLockSlim, BenchmarkReaderWriterLock, BenchmarkSpinLock,
                //BenchmarkManualResetEvent,
                //BenchmarkVoid,
                //BenchmarkVoid,
                //BenchmarkMonitor,
                //BenchmarkMonitor,
                //BenchmarkSpinLatch,
                //BenchmarkSpinLatch
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
            var dic = new ConcurrentDictionary<int, int>(ThreadCount, n);
            yield return "Concurrent Dictionary single thread sequential access";
            yield return BenchmarkAction($"Added {n:0,0} items to Dictionary", (i) => dic.TryAdd(i, i));
            yield return BenchmarkAction($"Read {n:0,0} items from Dictionary", i =>
            {
                dic.TryGetValue(i, out int v);
                if (v != i) throw new InvalidOperationException("Test failed");
            });
        }



        static IEnumerable<string> BenchmarkDictionaryRandom()
        {
            var dic = new ConcurrentDictionary<long, long>(ThreadCount, n); 
            var rnd = new Random(0);
            yield return "Concurrent Dictionary single thread random access ";
            yield return BenchmarkAction($"Added {n:0,0} items to Dictionary", (i) =>
            {
                long x;
                do
                {
                    x = rnd.Next();
                } while (dic.ContainsKey(x));
                dic.TryAdd(x, x);
            });
            rnd = new Random(0);
            yield return BenchmarkAction($"Read {n:0,0} items from Dictionary", i =>
            {
                long key = rnd.Next();
                dic.TryGetValue(key, out long value);
                if (value != key) throw new InvalidOperationException("Test failed");
            });
        }

        static IEnumerable<string> BenchmarkStaticFixedSizeHashTableRandom()
        {
            //return BenchmarkHashTable((key) => (ulong)(key), (hashTable, i) => hashTable.Add(i, i));
            string filePath = "Int64Int64.hash-table";
            if (File.Exists(filePath)) File.Delete(filePath);

            var rnd = new Random(0);
            using (var hashTable = new StaticFixedSizeHashTable<long, long>(filePath, n, ThreadSafety.Safe, key => key, null, false))
            {
                yield return "StaticFixedSizeHashTable thread safe single thread random access";
                yield return BenchmarkAction($"Added {n:0,0} items to HashTable", (i) =>
                {
                    long x;
                    do
                    {
                        x = rnd.Next();
                        long y = x;
                    } while (hashTable.ContainsKey(x));
                    hashTable.Add(x, x);
                });
                yield return $"HashTable MaxDistance:  { hashTable.MaxDistance}";
                yield return $"HashTable MeanDistance:  { hashTable.MeanDistance:0.0}";
                yield return BenchmarkAction("HashTable flushed", _ => hashTable.Flush(), 1);
                rnd = new Random(0);
                yield return BenchmarkAction($"Read {n:0,0} items from HashTable", i =>
                {
                    long key = rnd.Next();
                    hashTable.TryGetValue(key, out long value);
                    if (value != key) throw new InvalidOperationException("Test failed");
                });
            }
        }

        static IEnumerable<string> BenchmarkConcurrentDictionaryMultiThreaded()
        {
            yield return $"ConcurrentDictionary {ThreadCount} threads";
            var watch = Stopwatch.StartNew();
            var tasks = new Task[ThreadCount];
            var dictionary = new ConcurrentDictionary<long, long>(ThreadCount, n);
            for (int i = 0; i < ThreadCount; i++)
            {
                tasks[i] = DictionaryTryAdd(ThreadCount, dictionary, i);
            }
            Task.WaitAll(tasks);
            watch.Stop();
            yield return $"{n:0,0} ConcurrentDictionary.TryAdd calls in {watch.Elapsed}";
            watch.Restart();
            for (int i = 0; i < ThreadCount; i++)
            {
                tasks[i] = DictionaryTryGet(ThreadCount, dictionary, i);
            }
            Task.WaitAll(tasks);
            watch.Stop();
            yield return $"{n:0,0} ConcurrentDictionary.TryGetValue calls in {watch.Elapsed}";
        }

        static IEnumerable<string> BenchmarkStaticFixedSizeHashTableMultiThreaded()
        {
            string filePath = "Int64Int64.HashTable";
            if (File.Exists(filePath)) File.Delete(filePath);
            yield return $"StaticFixedSizeHashTable {ThreadCount} threads";
            var watch = Stopwatch.StartNew();
            var p = Process.GetCurrentProcess();
            using (var hashTable = new StaticFixedSizeHashTable<long, long>(filePath, n, ThreadSafety.Safe, key => key, null, false))
            {
                //hashTable.WarmUp();
                //watch.Stop();
                //yield return $"Warmed up in {watch.Elapsed}";
                //watch.Restart();
                var tasks = new Task[ThreadCount];
                for (int i= 0; i < ThreadCount; i++)
                {
                    tasks[i] = HashTableTryAdd(ThreadCount, hashTable, i);
                }
                Task.WaitAll(tasks);
                watch.Stop();
                yield return $"{n:0,0} HashTable.TryAdd calls in {watch.Elapsed}";

                watch.Restart();
                for (int i = 0; i < ThreadCount; i++)
                {
                    tasks[i] = HashTableTryGet(ThreadCount, hashTable, i);
                }
                Task.WaitAll(tasks);
                watch.Stop();
                yield return $"{n:0,0} HashTable.TryGetValue calls in {watch.Elapsed}";

                watch.Restart();
                for (int i = 0; i < ThreadCount; i++)
                {
                    tasks[i] = HashTableTryGetNonBlocking(ThreadCount, hashTable, i);
                }
                Task.WaitAll(tasks);
                watch.Stop();
                yield return $"{n:0,0} HashTable.TryGetValueNonBlocking calls in {watch.Elapsed}";
                yield return $"Peak Working Set {p.PeakWorkingSet64:0,0}; Private Memory {p.PrivateMemorySize64:0,0}; GC Total Memory ${GC.GetTotalMemory(false):0,0}";
                hashTable.Flush();
            }
            if (File.Exists(filePath)) File.Delete(filePath);
            //Thread.Sleep(2000);
            p = Process.GetCurrentProcess();
            yield return $"Memory after disposing hash table: Working Set: {p.WorkingSet64:0,0}";
        }

        private static Task HashTableTryAdd(int threadCount, StaticFixedSizeHashTable<long, long> hashTable, int i)
        {
           return Task.Factory.StartNew(() =>
           {
               var rnd = new Random((int)((long)i * (long)n / threadCount));
               int count = n / threadCount;
               int start = i * count;
               int end = start + count;
               for (int j = start; j < end; j++)
               {
                   var key = rnd.Next();
                   hashTable.TryAdd(key, key);
               }
           }, TaskCreationOptions.LongRunning);
        }


        private static Task HashTableTryGet(int threadCount, StaticFixedSizeHashTable<long, long> hashTable, int i)
        {
            return Task.Factory.StartNew(() =>
            {
                var rnd = new Random((int)((long)i * (long)n / threadCount));
                int count = n / threadCount;
                int start = i * count;
                int end = start + count;
                for (int j = start; j < end; j++)
                {
                    var key = rnd.Next();
                    hashTable.TryGetValue(key, out long v);
                }
            }, TaskCreationOptions.LongRunning);
        }

        private static Task HashTableTryGetNonBlocking(int threadCount, StaticFixedSizeHashTable<long, long> hashTable, int i)
        {
            return Task.Factory.StartNew(() =>
            {
                var rnd = new Random((int)((long)i * (long)n / threadCount));
                int count = n / threadCount;
                int start = i * count;
                int end = start + count;
                for (int j = start; j < end; j++)
                {
                    var key = rnd.Next();
                    hashTable.TryGetValueNonBlocking(key, out long v);
                }
            }, TaskCreationOptions.LongRunning);
        }

        private static Task DictionaryTryAdd(int threadCount, ConcurrentDictionary<long, long> dic, int i)
        {
            return Task.Factory.StartNew(() =>
            {
                var rnd = new Random((int)((long)i * (long)n / threadCount));
                int count = n / threadCount;
                int start = i * count;
                int end = start + count;
                for (int j = start; j < end; j++)
                {
                    var key = rnd.Next();
                    dic.TryAdd(key, key);
                }
            }, TaskCreationOptions.LongRunning);
        }

        private static Task DictionaryTryGet(int threadCount, ConcurrentDictionary<long, long> dic, int i)
        {
            return Task.Factory.StartNew(() =>
            {
                var rnd = new Random((int)((long)i * (long)n / threadCount));
                int count = n / threadCount;
                int start = i * count;
                int end = start + count;
                for (int j = start; j < end; j++)
                {
                    var key = rnd.Next();
                    dic.TryGetValue(j, out long v);
                }
            }, TaskCreationOptions.LongRunning);
        }

        static IEnumerable<string> BenchmarkStaticFixedSizeHashTableSequential()
        {
            //return BenchmarkHashTable((key) => (ulong)(key), (hashTable, i) => hashTable.Add(i, i));
            string filePath = "Int64Int64.hash-table";
            if (File.Exists(filePath)) File.Delete(filePath);
            using (var hashTable = new StaticFixedSizeHashTable<long, long>(filePath, n, ThreadSafety.Safe, key => key, null, false))
            {
                yield return "StaticFixedSizeHashTable thread safe single thread sequencial access";
                yield return BenchmarkAction($"Added {n:0,0} items to HashTable", (i) => hashTable.Add(i, i));
                yield return $"HashTable MaxDistance:  { hashTable.MaxDistance}";
                yield return $"HashTable MeanDistance:  { hashTable.MeanDistance:0.0}";
                yield return BenchmarkAction("HashTable flushed", _ => hashTable.Flush(), 1);
                yield return BenchmarkAction($"Read {n:0,0} items from HashTable", i =>
                {
                    hashTable.TryGetValue(i, out long v);
                    if (v != i) throw new InvalidOperationException("Test failed");
                });
            }
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
                readerWriterLock.EnterWriteLock();
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

        static IEnumerable<string> BenchmarkManualResetEvent()
        {
            ManualResetEvent manualResetEvent = new ManualResetEvent(true);
            yield return "ManualResetEvent";
            yield return BenchmarkAction($"Executed {n:0,0} times", _ =>
            {
                manualResetEvent.Reset();
                manualResetEvent.Set();
            }, n/10);
        }

        static IEnumerable<string> BenchmarkVoid()
        {
            yield return "Void";
            var watch = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            {
            }
            watch.Stop();
            yield return $"Executed Void in {watch.Elapsed}";
        }

        static IEnumerable<string> BenchmarkMonitor()
        {
            object syncObject = new object();
            yield return "Monitor no contention";
            var watch = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            {
                bool taken = false;
                try
                {
                    Monitor.Enter(syncObject, ref taken);
                }
                finally
                {
                    if (taken) Monitor.Exit(syncObject);
                }
            }
            watch.Stop();
            yield return $"Executed Monitor.Enter and Monitor.Exit in {watch.Elapsed}";
        }

        static IEnumerable<string> BenchmarkSpinLatch()
        {
            yield return "SpinLatch no contention";
            var watch = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            {
                bool taken = false; int locked = 0;
                SpinLatch.Enter(ref locked, ref taken);
                if (taken) SpinLatch.Exit(ref locked);
            }
            watch.Stop();
            yield return $"Executed {n:0,0} times in {watch.Elapsed}";
        }
    }
}
