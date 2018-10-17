using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct FixedSizeRobinHoodHashTableFileHeader
    {
        public const long MagicNumber = -2358176814029485096L;

        public long Magic;
        public ulong Slots;
        public ulong RecordCount;
        public uint KeySize;
        public uint ValueSize;
        public uint RecordSize;
        public bool IsAligned;
        public fixed byte Reserved[3];
    }
}
