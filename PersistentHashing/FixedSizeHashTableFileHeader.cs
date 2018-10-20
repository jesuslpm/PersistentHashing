using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct FixedSizeHashTableFileHeader
    {
        public const long MagicNumber = -2358176814029485096L;

        public long Magic;
        public long Slots;
        public long RecordCount;
        public int KeySize;
        public int ValueSize;
        public int RecordSize;
        public bool IsAligned;
        public fixed byte Reserved[3];
    }
}
