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
    public ref struct FileSlice
    {
        public Span<byte> Span;
        public long Offset;

        public FileSlice(Span<byte> span, long offset)
        {
            this.Span = span;
            this.Offset = offset;
        }
    }

    public ref struct FileItemSlice
    {
        public Span<byte> KeySpan;
        public Span<byte> ValueSpan;
        public long Offset;

        public FileItemSlice(Span<byte> keySpan, Span<byte> valueSpan, long offset)
        {
            this.KeySpan = keySpan;
            this.ValueSpan = valueSpan;
            this.Offset = offset;
        }
    }

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
                this.growthIncrement = (growthIncrement + Constants.AllocationGranularityMask) / Constants.AllocationGranularityMask;

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

        public long AllocateBytes(int bytesToAllocate, out byte* baseAddress)
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
                    }
                }
            }
            baseAddress = memoryMapper.mapping.GetBaseAddress();
            return newFreeSpaceOffset - bytesToAllocate;
        }

        public FileSlice AllocateValue(int size)
        {
            var valueSize = size > 0 ? size : 0;
            var offset = AllocateBytes(valueSize + sizeof(int), out byte *baseAddress);
            var address =baseAddress + offset;
            *(int*)address = size;
            return new FileSlice(size < 0 ? Span<byte>.Empty : new Span<byte>(address + sizeof(int), size), offset);
        }

        public FileItemSlice AllocateItem(int keySize, int valueSize )
        {
            var itemSize = (keySize > 0 ? keySize : 0) + (valueSize > 0 ? valueSize : 0);
            var offset = AllocateBytes(itemSize + 2* sizeof(int), out byte* baseAddress);
            var keyAddress = baseAddress + offset;
            *(int*)keyAddress = keySize;
            var valueAddress = keyAddress + sizeof(int) + keySize;
            *(int*)valueAddress = valueSize;
            return new FileItemSlice(
                keySize < 0 ? Span<byte>.Empty : new Span<byte>(keyAddress + sizeof(int), keySize), 
                valueSize < 0 ? Span<byte>.Empty : new Span<byte>(valueAddress + sizeof(int), valueSize), offset);
        }

        public void Free(long offset)
        {
            //TODO: implementing free space management.
        }

        //internal long Write(ReadOnlySpan<byte> value, byte* baseAddress)
        //{
        //    int bytesToAllocateAndCopy = value.Length + sizeof(int);
        //    var valueOffset = Allocate(bytesToAllocateAndCopy);
        //    byte* destination = baseAddress + valueOffset;
        //    *(int*)destination = value.Length;
        //    var destinationSpan = new Span<byte>(destination + sizeof(int), value.Length);
        //    value.CopyTo(destinationSpan);
        //    return valueOffset;
        //}

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
