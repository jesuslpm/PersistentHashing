using PersistentHashing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace InformalTests
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var hashTable = new FixedSizeRobinHoodPersistentHashTable<int, int>("Int32Int32.hash-table", 20000, true))
            {
                
            }

            using (var hashTable = new FixedSizeRobinHoodPersistentHashTable<Guid, int>("GuidInt32Table.hash-table", 20000, true))
            {

            }
        }
    }
}
