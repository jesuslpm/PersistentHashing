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
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace PersistentHashing
{
    public unsafe sealed class MemoryMapper : IDisposable
    {
        internal FileStream fs;
        internal MemoryMapping mapping;

        private HashSet<MemoryMappingSession> sessions = new HashSet<MemoryMappingSession>();

        internal readonly object SyncObject = new object();

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
            try
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
            catch
            {
                Dispose();
                throw;
            }
        }


        public bool IsDisposed { get; private set; }


        public void Dispose()
        {
            if (IsDisposed) return;
            lock (SyncObject)
            {
                if (IsDisposed) return;
                IsDisposed = true;
                List<Exception> exceptions = new List<Exception>();
                if (mapping != null)
                {
                    try
                    {
                        mapping.Release();
                        mapping = null;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
                if (fs != null)
                {
                    try
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
                if (exceptions.Count > 0) throw new AggregateException(exceptions);
            }
        }

        private void CheckDisposed()
        {
            if (IsDisposed) throw new ObjectDisposedException(nameof(MemoryMapper));
        }

        public byte* Grow(long bytesToGrow)
        {
            CheckDisposed();
            lock (SyncObject)
            {
                CheckDisposed();
                var newMapping = MemoryMapping.Grow(bytesToGrow, mapping);
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
                return mapping.GetBaseAddress();
            }
        }

        public MemoryMappingSession OpenSession()
        {
            CheckDisposed();
            lock (SyncObject)
            {
                CheckDisposed();
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
            lock (SyncObject)
            {
                CheckDisposed();
                FlushInternal();
            }
        }
    }
}