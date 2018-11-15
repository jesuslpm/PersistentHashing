/*
Copyright 2018 Jesús López Méndez

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
