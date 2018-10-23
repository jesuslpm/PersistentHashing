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

        static unsafe void Main(string[] args)
        {
            var locks = stackalloc bool[0];
            //BenchMark();
            Console.WriteLine("Press enter to exit..");
            Console.ReadLine();
        }


    }
}
