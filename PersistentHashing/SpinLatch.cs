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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    /*
     * It is not worth it. SpinLatch is only slightly faster than monitor.
     * So, we are not going to use it.
     */
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct SpinLatch
    {
        public volatile int Value;
        public bool IsLocked
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (Value >> 31) != 0;
        }

        public int Version
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Value & 0x7FFF_FFFF;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set => Value = (Value & unchecked((int)0x8000_0000u)) | value;
        }

        public void IncrementVersionCircularly()
        {
            int newVersion = unchecked(Version + 1);
            if (newVersion < 0) newVersion = 0;
            Version = newVersion;
        }

        public void Enter(ref bool taken)
        {
            SpinWait spinWait = new SpinWait();
            while (true)
            {
                try { }
                finally
                {
                    var currentVersion = Version;
                    int newValue = currentVersion & unchecked((int)0x8000_0000u);
                    taken = Interlocked.CompareExchange(ref Value, newValue, currentVersion) == currentVersion;
                }
                if (taken) return;
                spinWait.SpinOnce();
            }
        }

        public void Exit()
        {
            Value &= 0x7FFF_FFFF;
        }


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
            locked = 0;
        }
    }
}
