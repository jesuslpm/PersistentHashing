using PersistentHashing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace InformalTests
{

    class Program
    {
        const int n = 100_000_000;

        static unsafe void Main(string[] args)
        {
            var watch = Stopwatch.StartNew();
            for (int i =0; i < n; i++)
            {
                ThisMethodDoesNotInitializeStackAllocatedMemory();
            }
            watch.Stop();
            Console.WriteLine($"Elapsed {watch.Elapsed}");

            watch.Restart();
            for (int i = 0; i < n; i++)
            {
                ThisMethodAllocatesMemoryOnTheHeap();
            }
            watch.Stop();
            Console.WriteLine($"Elapsed {watch.Elapsed}");
        }

        private static unsafe string ThisMethodDoesNotInitializeStackAllocatedMemory()
        {
            // avoid declaring other local vars, or doing work with stackalloc
            // to prevent the .locals init cil flag , see: https://github.com/dotnet/coreclr/issues/1279
            char* pointer = stackalloc char[256];
            return CreateString(pointer, 256);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe string CreateString(char *pointer, int length)
        {
            return "";
        }

        private static unsafe string ThisMethodAllocatesMemoryOnTheHeap()
        {
            var a = new char[256];
            return "";
        }


    }
}
