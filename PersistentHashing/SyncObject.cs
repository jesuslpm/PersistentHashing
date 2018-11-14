using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public class SyncObject
    {
        public volatile bool IsWriterInProgress;
        public volatile int Version;
#if SPINLATCH
        public int Locked;
#endif
    }
}
