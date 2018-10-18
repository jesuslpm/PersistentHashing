using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public class SynchonizedOperation
    {
        private object syncObject = new object();
        private volatile int isWriterInProgress = 0;
        private volatile int version = 0;

        public TResult Write<TResult>(Func<TResult> func)
        {
            SpinWait? spinWait = null;
            bool isLatchAcquired = false;
            try
            {
                for(;;)
                {
                    try { }
                    finally
                    {
                        if (Interlocked.CompareExchange(ref isWriterInProgress, 1, 0) == 0)
                        {
                            isLatchAcquired = true;
                        }
                    }
                    if (isLatchAcquired) break;

                    if (!spinWait.HasValue) spinWait = new SpinWait();
                    spinWait.Value.SpinOnce();
                } 
                return func();
            }
            finally
            {
                if (isLatchAcquired)
                {
                    unchecked { version++; }
                    isWriterInProgress = 0;
                }
            }
        }
 

        public TResult Read<TResult>(Func<TResult> func)
        {
            int version;
            SpinWait? spinWait = null;
            for(;;)
            {
                if (isWriterInProgress == 1)
                {
                    if (!spinWait.HasValue) spinWait = new SpinWait();
                    spinWait.Value.SpinOnce();
                }
                version = this.version;
                var result = func();
                if (isWriterInProgress == 1 || version != this.version) continue;
                return result;
            } 
        }
    }
}
