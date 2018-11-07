﻿using System.Runtime.InteropServices;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct DataFileHeader
    {
        public const long MagicNumber = 4839648416978550723L;

        public long Magic;
        public long FreeSpaceOffset;
    }
}
