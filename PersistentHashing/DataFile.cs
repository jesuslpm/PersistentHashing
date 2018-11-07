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
            bool isNew = !File.Exists(filePath);
            memoryMapper = new MemoryMapper(filePath, initialSize);
            mapping = MemoryMapping.Create(memoryMapper.fs, Constants.AllocationGranularity);
            dataFileHeaderPointer = (DataFileHeader*) mapping.GetBaseAddress();
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

        public bool IsDisposed { get; private set; }

        private void Dispose(bool disposing)
        {
            if (IsDisposed) return;
            IsDisposed = true;
            mapping.Release();
            mapping = null;
            memoryMapper.Dispose();
            memoryMapper = null;
            if (disposing) GC.SuppressFinalize(this);
        }

        public void Flush()
        {
            CheckDisposed();
            memoryMapper.Flush();
        }

        public void Dispose()
        {
            Dispose(true);
        }

        ~DataFile()
        {
            Dispose(false);
        }
    }
}
