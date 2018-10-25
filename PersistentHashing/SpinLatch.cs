using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    /*
     * It is not worth it. SpinLatch is only slightly faster than monitor.
     * So, we are not going to use it.
     */
    public static class SpinLatch
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Enter(ref int locked, ref bool taken)
        {
            SpinWait spinWait = new SpinWait();
            while (true)
            {
                try { }
                finally
                {
                    taken = Interlocked.Exchange(ref locked, 1) == 0;
                }
                if (taken) return;
                spinWait.SpinOnce();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Exit(ref int locked)
        {
            Interlocked.Exchange(ref locked, 0);
        }
    }
}
