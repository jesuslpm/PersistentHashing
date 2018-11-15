using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct StaticHashTableFileHeader
    {
        public const long MagicNumber = -2358176814029485096L;

        public long Magic;
        public long SlotCount;
        public long RecordCount;
        public long DistanceSum;
        public int RecordSize;
        public volatile int MaxDistance;
    }
}
