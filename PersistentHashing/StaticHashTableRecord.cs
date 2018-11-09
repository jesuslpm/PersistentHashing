using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct StaticHashTableRecord<TK, TV> where TK : unmanaged where TV: unmanaged
    {
        public TK K;
        public TV V;
        public short Distance;
    }
}
