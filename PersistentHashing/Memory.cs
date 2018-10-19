using System;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    /*
    * Code in this class is taken from RavenDB, https://github.com/ravendb/ravendb
    * which is licensed as AGPLv3, https://github.com/ravendb/ravendb/blob/v4.1/LICENSE
    * Permission to use this code is pending.
    */
    public class Memory
    {
        
        public static unsafe int Compare(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = bpy - bpx;

            if ((size & 7) == 0)
                goto ProcessAligned;

            // We process first the "unaligned" size.
            ulong xor;
            if ((size & 4) != 0)
            {
                xor = *((uint*)bpx) ^ *((uint*)(bpx + offset));
                if (xor != 0)
                    goto Tail;

                bpx += 4;
            }

            if ((size & 2) != 0)
            {
                xor = (ulong)(*((ushort*)bpx) ^ *((ushort*)(bpx + offset)));
                if (xor != 0)
                    goto Tail;

                bpx += 2;
            }

            if ((size & 1) != 0)
            {
                int value = *bpx - *(bpx + offset);
                if (value != 0)
                    return value;

                bpx += 1;
            }

        ProcessAligned:

            byte* end = (byte*)p1 + size;
            byte* loopEnd = end - 16;
            while (bpx <= loopEnd)
            {
                // PERF: JIT will emit: ```{op} {reg}, qword ptr [rdx+rax]```
                if (*((ulong*)bpx) != *(ulong*)(bpx + offset))
                    goto XorTail;

                if (*((ulong*)(bpx + 8)) != *(ulong*)(bpx + 8 + offset))
                {
                    bpx += 8;
                    goto XorTail;
                }


                bpx += 16;
            }

            if (bpx < end)
                goto XorTail;

            return 0;

        XorTail: xor = *((ulong*)bpx) ^ *(ulong*)(bpx + offset);

        Tail:

            // Fast-path for equals
            if (xor == 0)
                return 0;

            // PERF: This is a bit twiddling hack. Given that bitwise xoring 2 values flag the bits difference, 
            //       we can use that we know we are running on little endian hardware and the very first bit set 
            //       will correspond to the first byte which is different. 

            bpx += Bits.TrailingZeroesInBytes(xor);
            return *bpx - *(bpx + offset);

        }
    }
}
