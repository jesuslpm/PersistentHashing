using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace PersistentHashing
{
    public unsafe sealed class MemoryMapper : IDisposable
    {

        private const int AllocationGranularity = 64 * 1024;

        private FileStream fs;
        internal MemoryMapping mapping;

        public long Length
        {
            get
            {
                CheckDisposed();
                return fs.Length;
            }
        }

        public MemoryMapper(string filePath, long initialFileSize)
        {
            if (initialFileSize <= 0 || initialFileSize % AllocationGranularity != 0)
            {
                throw new ArgumentException("The initial file size must be a multiple of 64Kb and grater than zero");
            }
            bool existingFile = File.Exists(filePath);
            fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            if (existingFile)
            {
                if (fs.Length <= 0 || fs.Length % AllocationGranularity != 0)
                {
                    throw new ArgumentException("Invalid file. Its lenght must be a multiple of 64Kb and greater than zero");
                }
            }
            else
            {
                fs.SetLength(initialFileSize);
            }
            mapping = MemoryMapping.Create(fs);
        }


        private bool isDisposed;

        public void Dispose()
        {
            if (isDisposed) return;
            isDisposed = true;
            mapping.Release();
            fs.Dispose();
        }

        private void CheckDisposed()
        {
            if (isDisposed) throw new ObjectDisposedException(this.GetType().Name);
        }

        public void Grow(long bytesToGrow)
        {
            CheckDisposed();
            var newMapping = MemoryMapping.Grow(bytesToGrow, this.mapping);
            if (mapping != newMapping)
            {
                var oldMapping = mapping;
                mapping = newMapping;
                oldMapping.Release();
            }
            //return newMapping;
        }

        public MemoryMappingSession OpenSession()
        {
            CheckDisposed();
            return new MemoryMappingSession(this);
        }

        public void Flush()
        {
            CheckDisposed();
            mapping.Flush();
            fs.Flush(true);
        }
    }
}