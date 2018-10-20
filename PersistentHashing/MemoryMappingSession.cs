using System;
using System.Collections.Generic;
using System.Text;

namespace PersistentHashing
{
    public unsafe sealed class MemoryMappingSession: IDisposable
    {
        private readonly MemoryMapper mapper;
        private List<MemoryMapping> mappings;
        private MemoryMapping mapping;

            
        internal MemoryMappingSession(MemoryMapper mapper)
        {
            this.mapper = mapper;
            mappings = new List<MemoryMapping>();
        }

        public bool IsDisposed { get; private set; }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (IsDisposed) return;
            IsDisposed = true;
            foreach (var mapping in mappings)
            {
                mapping.Release();
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

        public byte* GetBaseAddress()
        {
            if (IsDisposed) throw new ObjectDisposedException(nameof(MemoryMappingSession));
            var newMapping = mapper.mapping;
            if (mapping != newMapping)
            {
                mapping = newMapping;
                newMapping.AddRef();
                mappings.Add(newMapping);
            }
            return newMapping.GetBaseAddress();
        }
    }
}
