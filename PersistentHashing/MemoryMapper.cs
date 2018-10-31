using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace PersistentHashing
{
    public unsafe sealed class MemoryMapper : IDisposable
    {
        private FileStream fs;
        internal MemoryMapping mapping;

        private HashSet<MemoryMappingSession> sessions = new HashSet<MemoryMappingSession>();

        internal object sessionsSyncObject = new object();

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
            if (initialFileSize <= 0 || initialFileSize % Constants.AllocationGranularity != 0)
            {
                throw new ArgumentException("The initial file size must be a multiple of 64Kb and grater than zero");
            }
            bool existingFile = File.Exists(filePath);
            fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            if (existingFile)
            {
                if (fs.Length <= 0 || fs.Length % Constants.AllocationGranularity != 0)
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
            //this.FlushInternal();
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
            lock (sessionsSyncObject)
            {
                var newMapping = MemoryMapping.Grow(bytesToGrow, this.mapping);
                if (mapping != newMapping)
                {
                    var oldMapping = mapping;
                    mapping = newMapping;
                    oldMapping.Release();
                    foreach (var session in sessions)
                    {
                        session.AddMapping(newMapping);
                    }
                }
            }
        }

        public MemoryMappingSession OpenSession()
        {
            CheckDisposed();
            lock (sessionsSyncObject)
            {
                var session = new MemoryMappingSession(this);
                sessions.Add(session);
                return session;
            }
        }

        internal void RemoveSession(MemoryMappingSession session)
        {
            sessions.Remove(session);
        }

        private void FlushInternal()
        {
            mapping.Flush();
            fs.Flush(true);
        }

        public void Flush()
        {
            CheckDisposed();
            FlushInternal();
        }
    }
}