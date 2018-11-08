using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    internal unsafe struct OperationContext<TKey>
    {
        public long InitialSlot;
        public long RemainingSlotsInChunk;
        public long CurrentSlot;
        public TKey Key;
        public int LockIndex;
        public bool* TakenLocks;
        public bool IsWriting;
    }
}
