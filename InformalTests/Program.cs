using PersistentHashing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InformalTests
{
    class Program
    {
        static void Main(string[] args)
        {
            test();
            Console.WriteLine("Press enter to exit..");
            Console.ReadLine();
        }

        static void test()
        {
            const int n = 100_000_000;
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
            using (var hashTable = new FixedSizeRobinHoodPersistentHashTable<long, long>(filePath, n, (key) => (ulong) key, false))
            {
                for (int i = 0; i < n; i++)
                {
                    hashTable.Add(i, i);
                }
                Console.WriteLine($"FixedSizeRobinHoodPersistentHashTable<long, long> Elapsed time: {watch.Elapsed}");
                watch.Restart();

                hashTable.Flush();
                Console.WriteLine($"Flush FixedSizeRobinHoodPersistentHashTable<long, long> Elapsed time: {watch.Elapsed}");
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
