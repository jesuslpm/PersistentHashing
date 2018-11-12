using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct StaticHashTableRecord<TKeyOrHash, TValueOrOffset> where TKeyOrHash : unmanaged where TValueOrOffset : unmanaged
    {
        /// <summary>
        /// The key for fixed size and fixed key size hash tables.
        /// The key hash for variable size hash tables
        /// </summary>
        public TKeyOrHash KeyOrHash;
        /// <summary>
        /// The value for fixed size hash tables
        /// The data file offset of the value for fixed key size hash tables
        /// The data file offset of the key-value for variable size hash tables
        /// </summary>
        public TValueOrOffset ValueOrOffset;
        /// <summary>
        /// The distance plus one to the initial slot. 
        /// 0 means empty slot, 1 means 0 distance, 2 means 1 distance, and so on.
        /// </summary>
        public short Distance;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StaticHashTableRecord(TKeyOrHash keyOrHash, TValueOrOffset valueOrOffset, short distance)
        {
            this.KeyOrHash = keyOrHash;
            this.ValueOrOffset = valueOrOffset;
            this.Distance = distance;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StaticHashTableRecord(TKeyOrHash keyOrHash, TValueOrOffset valueOrOffset): this(keyOrHash, valueOrOffset, 0)
        {
        }
    }
}
