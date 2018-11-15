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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public unsafe class DataFile : IDisposable
    {
        MemoryMapper memoryMapper;
        MemoryMapping mapping;
        DataFileHeader* dataFileHeaderPointer;
        private readonly int growthIncrement;

        public DataFile(string filePath, long initialSize, int growthIncrement)
        {
            try
            {
                bool isNew = !File.Exists(filePath);
                memoryMapper = new MemoryMapper(filePath, initialSize);
                mapping = MemoryMapping.Create(memoryMapper.fs, Constants.AllocationGranularity);
                dataFileHeaderPointer = (DataFileHeader*)mapping.GetBaseAddress();
                growthIncrement = (growthIncrement + Constants.AllocationGranularityMask) / Constants.AllocationGranularityMask;

                if (isNew)
                {
                    InitializeHeader();
                }
                else
                {
                    ValidateHeader();
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        private void ValidateHeader()
        {
            if (dataFileHeaderPointer->Magic != DataFileHeader.MagicNumber)
            {
                throw new FormatException($"This is not a {nameof(DataFile)} file");
            }
        }

        private void InitializeHeader()
        {
            dataFileHeaderPointer->Magic = DataFileHeader.MagicNumber;
            dataFileHeaderPointer->FreeSpaceOffset = sizeof(DataFileHeader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (IsDisposed) throw new ObjectDisposedException(nameof(DataFile));
        }

        public MemoryMappingSession OpenSession()
        {
            CheckDisposed();
            return memoryMapper.OpenSession();
        }

        public long Allocate(int bytesToAllocate)
        {
            CheckDisposed();   
            long newFreeSpaceOffset = Interlocked.Add(ref dataFileHeaderPointer->FreeSpaceOffset, bytesToAllocate);
            if (newFreeSpaceOffset >= memoryMapper.Length)
            {
                lock (memoryMapper.SyncObject)
                {
                    if (Interlocked.Read(ref dataFileHeaderPointer->FreeSpaceOffset) > memoryMapper.Length)
                    {
                        long bytesToGrow = dataFileHeaderPointer->FreeSpaceOffset - memoryMapper.Length;
                        if (bytesToGrow < growthIncrement) bytesToGrow = growthIncrement;
                        bytesToGrow = (bytesToGrow + growthIncrement - 1) / growthIncrement;
                        memoryMapper.Grow(bytesToGrow);
                    }
                }
            }
            return newFreeSpaceOffset - bytesToAllocate;
        }

        public void Free(long offset)
        {
            //TODO: implementing free space management.
        }

        internal long Write(ReadOnlySpan<byte> value, byte* baseAddress)
        {
            int bytesToAllocateAndCopy = value.Length + sizeof(int);
            var valueOffset = Allocate(bytesToAllocateAndCopy);
            byte* destination = baseAddress + valueOffset;
            *(int*)destination = value.Length;
            var destinationSpan = new Span<byte>(destination + sizeof(int), value.Length);
            value.CopyTo(destinationSpan);
            return valueOffset;
        }

        public bool IsDisposed { get; private set; }

        public void Dispose()
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
            if (memoryMapper != null)
            {
                try
                {
                    memoryMapper.Dispose();
                    memoryMapper = null;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (exceptions.Count > 0) throw new AggregateException(exceptions);
        }

        public void Flush()
        {
            CheckDisposed();
            memoryMapper.Flush();
        }
    }
}
