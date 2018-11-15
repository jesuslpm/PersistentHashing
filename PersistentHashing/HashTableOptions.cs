using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public class BaseHashTableOptions<TKey, TValue>
    {
        public Func<TKey, long> HashFunction;
        public IEqualityComparer<TKey> KeyComparer;
        public IEqualityComparer<TValue> ValueComparer;
    }

    public class HashTableComparers<TKey, TValue>
    {
        public IEqualityComparer<TKey> KeyComparer;
        public IEqualityComparer<TValue> ValueComparer;
    }

    public class FixedKeySizeHashTableOptions<TKey>
    {
        public IEqualityComparer<TKey> KeyComparer;
        public long InitialDataFileSize;
        public int DataFileSizeGrowthIncrement;
    }


    public class VariableSizeHashTableOptions
    {
        public long InitialDataFileSize;
        public int DataFileSizeGrowthIncrement;
        public Func<MemorySlice, long> HashFunction;
    }
}
