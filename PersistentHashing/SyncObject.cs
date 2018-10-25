using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    internal class SyncObject
    {
        public volatile bool IsWriterInProgress;
        public volatile int Version;
    }
}
