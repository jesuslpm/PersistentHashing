using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    internal unsafe struct NonBlockingOperationContext<TKey>
    {
        public long InitialSlot;
        public int* Versions;
        public int VersionIndex;
        public TKey Key;
    }
}
