using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace PersistentHashing
{
    public partial class Hashing
    {

        public static int Fnv1a(ReadOnlySpan<byte> key, int seed = 0)
        {
            uint h = (uint)seed;
            h ^= 2166136261u;
            for (int i = 0; i < key.Length; i++)
            {
                h ^= key[i];
                h *= 16777619;
            }
            return (int)h;
        }

        public static int Fnv1a(int key, int seed = 0)
        {
            uint h = (uint)seed;
            h ^= 2166136261u;
            h ^= (uint)key & 0xFFu;
            h *= 16777619;
            h ^= ((uint)key >> 8) & 0xFFu;
            h *= 16777619;
            h ^= ((uint)key >> 16) & 0xFFu;
            h *= 16777619;
            h ^= ((uint)key >> 24) & 0xFFu;
            h *= 16777619;
            return (int)h;
        }

        public static int Fnv1aYt(int key, int seed = 0)
        {
            const uint PRIME = 709607;
            const uint FNV_OFFSET = 2166136261u;
            uint hash32a = (uint)seed ^ FNV_OFFSET;
            uint hash32b = FNV_OFFSET + 4;
            hash32a = (hash32a ^ ((uint)key & 0xFFFFu)) * PRIME;
            hash32b = (hash32b ^ ((uint)key >> 16)) * PRIME;
            hash32a = (hash32a ^ Bits.RotateLeft32(hash32b, 5)) * PRIME;
            return (int)(hash32a ^ (hash32a >> 16));
        }

        public static int Fnv1aYt(long key, int seed = 0)
        {
            const uint PRIME = 709607;
            const uint FNV_OFFSET = 2166136261u;
            uint hash32a = (uint)seed ^ FNV_OFFSET;
            uint hash32b = FNV_OFFSET + 8;
            hash32a = (hash32a ^ (uint)((ulong)key & 0xFFFFFFFFul)) * PRIME;
            hash32b = (hash32b ^ (uint)((ulong)key >> 32)) * PRIME;
            hash32a = (hash32a ^ Bits.RotateLeft32(hash32b, 5)) * PRIME;
            return (int)(hash32a ^ (hash32a >> 16));
        }

        public static int Fnv1aYt(ReadOnlySpan<byte> key, int seed = 0)
        {
            const uint PRIME = 709607;
            const uint FNV_OFFSET = 2166136261u;
            uint len = (uint)key.Length;
            uint hash32a = (uint)seed ^ FNV_OFFSET;
            uint hash32b = FNV_OFFSET + len;
            uint hash32c = FNV_OFFSET;

            var keyAsUints = MemoryMarshal.Cast<byte, uint>(key);
            var keyAsUshorts = MemoryMarshal.Cast<byte, ushort>(key);
            int offset;
            for (offset = 0; len >= 24; len -= 24)
            {
                hash32a = (hash32a ^ Bits.RotateLeft32(keyAsUints[offset++], 5) ^ keyAsUints[offset++]) * PRIME;
                hash32b = (hash32b ^ Bits.RotateLeft32(keyAsUints[offset++], 5) ^ keyAsUints[offset++]) * PRIME;
                hash32c = (hash32c ^ Bits.RotateLeft32(keyAsUints[offset++], 5) ^ keyAsUints[offset++]) * PRIME;
            }
            if (offset != 0)
            {
                hash32a = (hash32a ^ Bits.RotateLeft32(hash32c, 5)) * PRIME;
            }
            if ((len & 16) != 0)
            {
                hash32a = (hash32a ^ Bits.RotateLeft32(keyAsUints[offset++], 5) ^ keyAsUints[offset++]) * PRIME;
                hash32b = (hash32b ^ Bits.RotateLeft32(keyAsUints[offset++], 5) ^ keyAsUints[offset++]) * PRIME;
            }
            if ((len & 8) != 0)
            {
                hash32a = (hash32a ^ keyAsUints[offset++]) * PRIME;
                hash32b = (hash32b ^ keyAsUints[offset++]) * PRIME;
            }
            var ushortOffset = 2 * offset;
            if ((len & 4) != 0)
            {
                hash32a = (hash32a ^ keyAsUshorts[ushortOffset++]) * PRIME;
                hash32b = (hash32b ^ keyAsUshorts[ushortOffset++]) * PRIME;
            }
            if ((len & 2) != 0)
            {
                hash32a = (hash32a ^ (keyAsUshorts[ushortOffset++])) * PRIME;
            }
            if ((len & 1) != 0)
            {
                hash32a = (hash32a ^ key[ushortOffset * 2]) * PRIME;
            }
            hash32a = (hash32a ^ Bits.RotateLeft32(hash32b, 5)) * PRIME;
            return (int)(hash32a ^ (hash32a >> 16));
        }


        public static int X17(ReadOnlySpan<byte> key, int seed)
        {
            unchecked
            {
                uint h = (uint)seed;
                for (int i = 0; i < key.Length; i++)
                {
                    h = 17 * h + (key[i] - 32u);
                }
                return (int)h;
            }
        }

        public static int Berstein(ReadOnlySpan<byte> key, int seed = 5381)
        {
            uint h = (uint)seed;
            for (int i = 0; i < key.Length; i++)
            {
                h = 33 * h + key[i];
            }
            return (int)h;
        }

        public static int Sdbm(ReadOnlySpan<byte> key, int seed)
        {
            uint h = (uint)seed;
            for (int i = 0; i < key.Length; i++)
            {
                h = (h << 6) + (h << 16) - h + key[i];
            }
            return (int)h;
        }

        public static int XXHash32(int key, int seed = 0)
        {
            const uint PRIME32_2 = 2246822519U;
            const uint PRIME32_3 = 3266489917U;
            const uint PRIME32_4 = 668265263U;
            const uint PRIME32_5 = 374761393U;
            unchecked
            {
                uint h32 = (uint)seed + PRIME32_5 + 4;
                h32 += (uint)key * PRIME32_3;
                h32 = Bits.RotateLeft32(h32, 17) * PRIME32_4;
                h32 ^= h32 >> 15;
                h32 *= PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= PRIME32_3;
                h32 ^= h32 >> 16;
                return (int)h32;
            }
        }


        public static int XXHash32(long key, int seed = 0)
        {
            const uint PRIME32_2 = 2246822519U;
            const uint PRIME32_3 = 3266489917U;
            const uint PRIME32_4 = 668265263U;
            const uint PRIME32_5 = 374761393U;
            unchecked
            {
                uint h32 = (uint)seed + PRIME32_5 + 8;
                h32 += (uint)(key & 0xFFFFFFFF) * PRIME32_3;
                h32 = Bits.RotateLeft32(h32, 17) * PRIME32_4;
                h32 += (uint)(key >> 32) * PRIME32_3;
                h32 = Bits.RotateLeft32(h32, 17) * PRIME32_4;
                h32 ^= h32 >> 15;
                h32 *= PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= PRIME32_3;
                h32 ^= h32 >> 16;
                return (int)h32;
            }
        }

        public static int XXHash32(ReadOnlySpan<byte> key, int seed = 0)
        {
            const uint PRIME32_1 = 2654435761U;
            const uint PRIME32_2 = 2246822519U;
            const uint PRIME32_3 = 3266489917U;
            const uint PRIME32_4 = 668265263U;
            const uint PRIME32_5 = 374761393U;
            var keyAsUints = MemoryMarshal.Cast<byte, uint>(key);
            unchecked
            {
                uint h32;
                int remainingBytes = key.Length;
                int uintIndex = 0;
                if (remainingBytes >= 16)
                {
                    uint v1 = (uint)seed + PRIME32_1 + PRIME32_2;
                    uint v2 = (uint)seed + PRIME32_2;
                    uint v3 = (uint)seed + 0;
                    uint v4 = (uint)seed - PRIME32_1;
                    do
                    {
                        v1 += keyAsUints[uintIndex++] * PRIME32_2;
                        v2 += keyAsUints[uintIndex++] * PRIME32_2;
                        v3 += keyAsUints[uintIndex++] * PRIME32_2;
                        v4 += keyAsUints[uintIndex++] * PRIME32_2;

                        remainingBytes -= 16;

                        v1 = Bits.RotateLeft32(v1, 13);
                        v2 = Bits.RotateLeft32(v2, 13);
                        v3 = Bits.RotateLeft32(v3, 13);
                        v4 = Bits.RotateLeft32(v4, 13);

                        v1 *= PRIME32_1;
                        v2 *= PRIME32_1;
                        v3 *= PRIME32_1;
                        v4 *= PRIME32_1;
                    }
                    while (remainingBytes >= 16);

                    h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                }
                else
                {
                    h32 = (uint)seed + PRIME32_5;
                }

                h32 += (uint)key.Length;

                while (remainingBytes >= 4)
                {
                    h32 += keyAsUints[uintIndex++] * PRIME32_3;
                    h32 = Bits.RotateLeft32(h32, 17) * PRIME32_4;
                    remainingBytes -= 4;
                }

                int byteIndex = uintIndex * 4;
                while (remainingBytes != 0)
                {
                    h32 += key[byteIndex++] * PRIME32_5;
                    h32 = Bits.RotateLeft32(h32, 11) * PRIME32_1;
                    remainingBytes--;
                }

                h32 ^= h32 >> 15;
                h32 *= PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= PRIME32_3;
                h32 ^= h32 >> 16;

                return (int)h32;
            }

            /*
 
             */
        }

        public static unsafe ulong MetroHash64(byte* buffer, uint length, ulong seed=0)
        {
            const ulong k0 = 0xD6D018F5;
            const ulong k1 = 0xA2AA033B;
            const ulong k2 = 0x62992FC1;
            const ulong k3 = 0x30BC5B29;
            byte* ptr = buffer;
            byte* end = ptr + length;
            ulong h = (seed + k2) * k0;
            if (length >= 32)
            {
                ulong* v = stackalloc ulong[4]; v[0] = h; v[1] = h; v[2] = h; v[3] = h;
                do
                {
                    v[0] = *(ulong*)ptr * k0; ptr += 8; v[0] = Bits.RotateRight64(v[0], 29) + v[2];
                    v[1] = *(ulong*)ptr * k1; ptr += 8; v[1] = Bits.RotateRight64(v[1], 29) + v[3];
                    v[2] = *(ulong*)ptr * k2; ptr += 8; v[2] = Bits.RotateRight64(v[2], 29) + v[0];
                    v[3] = *(ulong*)ptr * k3; ptr += 8; v[3] = Bits.RotateRight64(v[3], 29) + v[1];
                }
                while (ptr <= (end - 32));
                v[2] ^= Bits.RotateRight64(((v[0] + v[3]) * k0) + v[1], 37) * k1;
                v[3] ^= Bits.RotateRight64(((v[1] + v[2]) * k1) + v[0], 37) * k0;
                v[0] ^= Bits.RotateRight64(((v[0] + v[2]) * k0) + v[3], 37) * k1;
                v[1] ^= Bits.RotateRight64(((v[1] + v[3]) * k1) + v[2], 37) * k0;
                h += v[0] ^ v[1];
            }
            if ((end - ptr) >= 16)
            {
                ulong v0 = h + (*(ulong*)ptr * k2); ptr += 8; v0 = Bits.RotateRight64(v0, 29) * k3;
                ulong v1 = h + (*(ulong*)ptr * k2); ptr += 8; v1 = Bits.RotateRight64(v1, 29) * k3;
                v0 ^= Bits.RotateRight64(v0 * k0, 21) + v1;
                v1 ^= Bits.RotateRight64(v1 * k3, 21) + v0;
                h += v1;
            }
            if ((end - ptr) >= 8)
            {
                h += *(ulong*)ptr * k3; ptr += 8;
                h ^= Bits.RotateRight64(h, 55) * k1;
            }
            if ((end - ptr) >= 4)
            {
                h += *(uint*)(ptr) * k3; ptr += 4;
                h ^= Bits.RotateRight64(h, 26) * k1;
            }
            if ((end - ptr) >= 2)
            {
                h += *(ushort*)(ptr) * k3; ptr += 2;
                h ^= Bits.RotateRight64(h, 48) * k1;
            }
            if ((end - ptr) >= 1)
            {
                h += *ptr * k3;
                h ^= Bits.RotateRight64(h, 37) * k1;
            }
            h ^= Bits.RotateRight64(h, 28);
            h *= k0;
            h ^= Bits.RotateRight64(h, 29);
            return h;
        }
    }
}
