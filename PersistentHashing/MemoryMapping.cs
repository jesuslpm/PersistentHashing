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
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;

namespace PersistentHashing
{
    internal unsafe class MemoryMapping
    {
        private int refCount;
        private List<MemoryMappedArea> areas = new List<MemoryMappedArea>();
        private FileStream fileStream;
        private volatile byte* baseAddress;

        private object syncObject = new object();


        private unsafe struct MemoryMappedArea
        {
            public MemoryMappedFile Mmf;
            public byte* Address;
            public long Size;

            public MemoryMappedArea(MemoryMappedFile mmf, byte* address, long size)
            {
                this.Mmf = mmf;
                this.Address = address;
                this.Size = size;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetBaseAddress()
        {
            return baseAddress;
        }

        internal static MemoryMapping Create(FileStream fs, long bytesToMap = 0)
        {
            var mmf = MemoryMappedFile.CreateFromFile(fs, null, fs.Length, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, true);
            var address = Win32FileMapping.MapViewOfFileEx(mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                Win32FileMapping.FileMapAccess.Read | Win32FileMapping.FileMapAccess.Write,
                0, 0, new UIntPtr((ulong)bytesToMap), null);
            if (address == null) throw new Win32Exception();

            var mapping = new MemoryMapping()
            {
                refCount = 1,
                fileStream = fs,
                baseAddress = address
            };
            mapping.areas.Add(new MemoryMappedArea(mmf, address, fs.Length));
            return mapping;
        }

        internal static MemoryMapping Grow(long bytesToGrow, MemoryMapping mapping)
        {
           
            if (bytesToGrow <= 0 || bytesToGrow % Constants.AllocationGranularity != 0)
            {
                throw new ArgumentException("The growth must be a multiple of 64Kb and greater than zero");
            }
            long offset = mapping.fileStream.Length;
            mapping.fileStream.SetLength(mapping.fileStream.Length + bytesToGrow);
            var mmf = MemoryMappedFile.CreateFromFile(mapping.fileStream, null, mapping.fileStream.Length, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, true);
            uint* offsetPointer = (uint*)&offset;
            var lastArea = mapping.areas[mapping.areas.Count - 1];
            byte* desiredAddress = lastArea.Address + lastArea.Size;
            ulong bytesToMap = (ulong)bytesToGrow;
            var address = Win32FileMapping.MapViewOfFileEx(mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                Win32FileMapping.FileMapAccess.Read | Win32FileMapping.FileMapAccess.Write,
                offsetPointer[1], offsetPointer[0], new UIntPtr(bytesToMap), desiredAddress);
            
            if (address == null)
            {
                bytesToMap = (ulong)mapping.fileStream.Length;
                address = Win32FileMapping.MapViewOfFileEx(mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                   Win32FileMapping.FileMapAccess.Read | Win32FileMapping.FileMapAccess.Write,
                   0, 0, new UIntPtr(bytesToMap), null);
                if (address == null) throw new Win32Exception();
                mapping = new MemoryMapping()
                {
                    baseAddress = address,
                    fileStream = mapping.fileStream,
                    refCount = 1
                };
            }
            
            var area = new MemoryMappedArea
            {
                Address = address,
                Mmf = mmf,
                Size = (long) bytesToMap
            };
            mapping.areas.Add(area);
            return mapping;
        }

        internal void Release()
        {
            lock (syncObject)
            {
                CheckDisposed();
                refCount--;
                if (refCount == 0) Dispose(true, false);
            }
        }

        private void Dispose(bool disposing, bool takeLock = true)
        {
            bool lockTaken = false;
            if (takeLock) Monitor.Enter(syncObject, ref lockTaken);
            try
            {
                if (IsDisposed) return;
                IsDisposed = true;
                var exceptions = new List<Exception>();
                foreach (var a in areas)
                {
                    try
                    {
                        if (Win32FileMapping.UnmapViewOfFile(a.Address) == false)
                        {
                            throw new Win32Exception();
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                    if (disposing)
                    {
                        try
                        {
                            a.Mmf.Dispose();
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                        }
                    }
                }
                if (disposing && exceptions.Count > 0) throw new AggregateException(exceptions);
            }
            finally
            {
                if (lockTaken) Monitor.Exit(syncObject);
            }
            if (disposing) GC.SuppressFinalize(this);
        }

        public void Dispose()
        {
            Dispose(true, true);
        }

        private void CheckDisposed()
        {
            if (IsDisposed) throw new ObjectDisposedException(nameof(MemoryMapping));
        }

        public bool IsDisposed { get; private set; }


        internal void AddRef()
        {
            lock (syncObject)
            {
                CheckDisposed();
                refCount++;
            }
        }

        internal void Flush()
        {
            lock (syncObject)
            {
                CheckDisposed();
                foreach (var area in areas)
                {
                    if (!Win32FileMapping.FlushViewOfFile(area.Address, new IntPtr(area.Size)))
                    {
                        throw new Win32Exception();
                    }
                }
            }
        }

        ~MemoryMapping()
        {
            Dispose(false, false);
        }
    }
}
