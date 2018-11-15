﻿/*
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
            locked = 0;
        }
    }
}
