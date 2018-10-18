using PersistentHashing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace InformalTests
{
    class Program
    {
        const int n = 10_000_000;

        static void Main(string[] args)
        {
            SynchonizedOperationTest();
            Console.WriteLine("Press enter to exit..");
            Console.ReadLine();
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

        static void testReaderWriterSlim()
        {
            var readerWriterLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            var spinLock = new SpinLock();
           
            var watch = Stopwatch.StartNew();
            var sync = new object();

            var operation = new SynchonizedOperation();

            var waitHandle = new ManualResetEvent(true);
            Func<int> func = () => 1;
            for (var i = 0; i < n; i++)
            {
                //bool lockTaken = false;
                //spinLock.Enter(ref lockTaken);
                //if (lockTaken) spinLock.Exit();
                //readerWriterLock.EnterReadLock();
                //readerWriterLock.ExitReadLock();

                waitHandle.Reset();
                waitHandle.Set();
                //operation.Write(func);
            }
            watch.Stop();
            Console.WriteLine($"TestReaderWriterSlim elapsed time: {watch.Elapsed}");
        }

        static void test()
        {
            const int n = 10_000_000;
            string filePath = "Int64Int64.hash-table";
            if (File.Exists(filePath)) File.Delete(filePath);
            var watch = Stopwatch.StartNew();
            //var dic = new Dictionary<int, int>(n);
            //for (int i = 0; i < n; i++)
            //{
            //    dic.Add(i, i);
            //}
            //Console.WriteLine($"Dictionary<int, int> Elapsed time: {watch.Elapsed}");
            Console.WriteLine($"Doing {n} inserts ...");
            watch.Restart();
            using (var hashTable = new FixedSizeRobinHoodPersistentHashTable<long, long>(filePath, n, (key) => Hashing.FastHashMix((ulong) key), null, false))
            {
                for (int i = 0; i < n; i++)
                {
                    hashTable.Add(i, i);
                }
                Console.WriteLine($"FixedSizeRobinHoodPersistentHashTable<long, long> Elapsed time: {watch.Elapsed}, MaxDistance: {hashTable.MaxDistance}");
                watch.Restart();

                hashTable.Flush();
                Console.WriteLine($"Flush elapsed time: {watch.Elapsed}");
                watch.Restart();

                Console.WriteLine($"Reading {n} records");
                for (int i = 0; i < n; i++)
                {
                    hashTable.TryGet(i, out long v);
                    if (v != i) throw new InvalidOperationException("Test failed");
                }
                Console.WriteLine($"FixedSizeRobinHoodPersistentHashTable<long, long> Elapsed time: {watch.Elapsed}");
                watch.Restart();
            }
        }
    }
}
