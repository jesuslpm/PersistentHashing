/* The MIT License
   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)
   Permission is hereby granted, free of charge, to any person
   obtaining a copy of this software and associated documentation
   files (the "Software"), to deal in the Software without
   restriction, including without limitation the rights to use, copy,
   modify, merge, publish, distribute, sublicense, and/or sell copies
   of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:
   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

using System.Runtime.CompilerServices;

namespace PersistentHashing
{
    // direct translation from https://code.google.com/archive/p/fast-hash/
    public partial class Hashing
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ulong FastHashMix(ulong h)
        {
            h ^= h >> 23;
            h *= 0x2127599bf4325c37UL;
            h ^= h >> 47;
            return h;
        }

        public static unsafe ulong FastHash64(byte* buffer, uint length, ulong seed=0)
        {
            const ulong m = 0x880355f21e6d1965UL;
            ulong* pos = (ulong*)buffer;
            ulong* end = pos + (length / 8);
            byte* pos2;
            ulong h = seed ^ (length * m);
            ulong v;
            while (pos != end)
            {
                v = *pos++;
                h ^= FastHashMix(v);
                h *= m;
            }
            pos2 = (byte*)pos;
            v = 0;
            switch (length & 7)
            {
                case 7: v ^= (ulong)pos2[6] << 48; goto case 6;
                case 6: v ^= (ulong)pos2[5] << 40; goto case 5;
                case 5: v ^= (ulong)pos2[4] << 32; goto case 4;
                case 4: v ^= (ulong)pos2[3] << 24; goto case 3;
                case 3: v ^= (ulong)pos2[2] << 16; goto case 2;
                case 2: v ^= (ulong)pos2[1] << 8; goto case 1;
                case 1:
                    v ^= (ulong)pos2[0];
                    h ^= FastHashMix(v);
                    h *= m;
                    break;
            }
            return FastHashMix(h);
        }

        public static unsafe uint FastHash32(byte* buffer, uint length, ulong seed = 0)
        {
            // the following trick converts the 64-bit hashcode to Fermat
            // residue, which shall retain information from both the higher
            // and lower parts of hashcode.
            ulong h = FastHash64(buffer, length, seed);
            return (uint) (h - (h >> 32));
        }
    }
}
