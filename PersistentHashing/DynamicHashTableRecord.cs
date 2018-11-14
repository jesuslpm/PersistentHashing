using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PersistentHashing
{
    [StructLayout(LayoutKind.Explicit)]
    public struct DynamicTableRecord
    {
        [FieldOffset(0)]
        public volatile int LowInt32;
        [FieldOffset(0)]
        public long Value;

        /*
           64 bits distributed as follows:

           Locked: 1 bit (bit 0, least significant bit)
           RehashingLevel: 7 bits (bits 1 to 7)
           DataOffset: 56 bits (bit 8 to 63)
                      
        */


        public int Locked
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return (LowInt32 & 1);
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
#if DEBUG
                if (value != 0 & value != 1) throw new ArgumentException($"Only 0 and 1 values are allowed, but you provided {value}");
#endif
                LowInt32 = (LowInt32 & (value | unchecked((int)0xFFFF_FFFE))) | value;
            }
        }

        public int RehashingLevel
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return (LowInt32 >> 1) & 0x7F;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
#if DEBUG
                if (value < 0 | value > 64) throw new ArgumentException($"value must be between 0 and 64, but you provided {value}");
#endif
                int n = value << 1;
                LowInt32 = (LowInt32 & (n | unchecked((int)0xFFFF_FF01))) | n;
            }
        }

        public long DataOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return Value >> 8;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                long n = value << 8;
                this.Value = (this.Value & (n | 0xFFL)) | n;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Lock(ref bool taken)
        {
            var spinWait = new SpinWait();
            while (true)
            {
                int comparisonValue = LowInt32 & unchecked((int)0xFFFF_FFFE);
                try { }
                finally
                {
                    taken = comparisonValue == Interlocked.CompareExchange(ref LowInt32, comparisonValue | 1, comparisonValue) ;
                }
                if (taken) return;
                spinWait.SpinOnce();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseLock()
        {
            LowInt32 &= unchecked((int)0xFFFF_FFFE);
        }
    }
}
