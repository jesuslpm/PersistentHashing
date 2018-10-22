using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    static class SpinLatch
    {
        static readonly long Timeout = TimeSpan.FromSeconds(10).Ticks;


        static void Enter(ref int locked, ref bool taken)
        {
            if (taken) throw new ArgumentException("taken must me false", nameof(taken));
            var spinWait = new SpinWait();
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

        static void Exit(ref int locked)
        {
            Interlocked.Exchange(ref locked, 0);
        }

        static void Enter(ref int locked, ref long lockedTime, object scope)
        {
            var spinWait = new SpinWait();
            while (Interlocked.Exchange(ref locked, 1) == 1)
            {
                var currentTime = DateTime.UtcNow.Ticks;
                // timed out
                long lockedTimeRead = Interlocked.Read(ref lockedTime);
                if (currentTime -  lockedTimeRead > Timeout)
                {
                    lock (scope)
                    {
                        Interlocked.Exchange(ref lockedTime, DateTime.UtcNow.Ticks);
                        return;
                    }
                }
                spinWait.SpinOnce();
            }
            Interlocked.Exchange(ref lockedTime, DateTime.UtcNow.Ticks);
        }
    }
}
