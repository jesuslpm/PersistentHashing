using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public unsafe sealed class MemoryMappingSession: IDisposable
    {
        public class BaseAddressChangedEventArgs : EventArgs
        {
            public readonly byte* BaseAddress;

            public BaseAddressChangedEventArgs(byte *baseAddress)
            {
                this.BaseAddress = baseAddress;
            }
        }

        public event EventHandler<BaseAddressChangedEventArgs> BaseAddressChanged;

        private readonly MemoryMapper mapper;
        private List<MemoryMapping> mappings;

        private volatile byte* baseAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetBaseAddress()
        {
            if (IsDisposed) throw new ObjectDisposedException(nameof(MemoryMappingSession));
            return baseAddress;
        }

        internal MemoryMappingSession(MemoryMapper mapper)
        {
            this.mapper = mapper;
            mappings = new List<MemoryMapping>();
            AddMapping(mapper.mapping);
        }

        public bool IsDisposed { get; private set; }

        public void Dispose()
        {
            Dispose(true);
        }

        internal void AddMapping(MemoryMapping mapping)
        {
            try { }
            // prevent ThreadAbortException from corrupting mappings collection
            finally
            {
                mappings.Add(mapping);
                mapping.AddRef();
                baseAddress = mapping.GetBaseAddress();
            }
            BaseAddressChanged?.Invoke(this, new BaseAddressChangedEventArgs(baseAddress));
        }

        private void Dispose(bool disposing)
        {
            bool lockTaken = false;
            try
            {
                // Need to lock because mapper might be calling AddMapping concurrently.
                // but we don't want to lock if being in the finalizer.
                if (disposing) Monitor.Enter(mapper.sessionsSyncObject, ref lockTaken);
                if (IsDisposed) return;
                IsDisposed = true;
                foreach (var mapping in mappings)
                {
                    mapping.Release();
                }
                mapper.RemoveSession(this);
                mappings = null;
                baseAddress = null;
            }
            finally
            {
                if (lockTaken) Monitor.Exit(mapper.sessionsSyncObject);
            }
            if (disposing)
            {
                GC.SuppressFinalize(this);
            }
        }

        ~MemoryMappingSession()
        {
            Dispose(false);
        }


        public void WarmUp()
        {
            byte* baseAddress = GetBaseAddress();
            void* endPointer = baseAddress + mapper.Length;
            long* pointer = (long*)baseAddress;
            long value;
            while (pointer < endPointer )
            {
                value = *pointer;
                pointer += 512;
            }
        }
    }
}
