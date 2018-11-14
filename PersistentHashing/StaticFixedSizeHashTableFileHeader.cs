using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public unsafe struct StaticFixedSizeHashTableFileHeader
    {
        public const long MagicNumber = -2358176814029485096L;

        public long Magic;
        public long SlotCount;
        public long RecordCount;
        public long DistanceSum;
        public int KeySize;
        public int ValueSize;
        public int RecordSize;
        public volatile int MaxDistance;
        public bool IsAligned;
        public fixed byte Reserved[7];
    }
}
