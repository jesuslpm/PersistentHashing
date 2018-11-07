using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public class HashTableOptions<TKey, TValue>
    {
        public Func<TKey, long> HashFunction;
        public IEqualityComparer<TKey> KeyComparer;
        public IEqualityComparer<TValue> ValueComparer;
        public bool IsAligned;
    }
}
