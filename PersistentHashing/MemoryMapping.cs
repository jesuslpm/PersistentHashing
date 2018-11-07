using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    internal unsafe class MemoryMapping
    {


        private int refCount;
        private List<MemoryMappedArea> areas = new List<MemoryMappedArea>();
        private FileStream fileStream;
        private byte* baseAddress;


        private unsafe class MemoryMappedArea
        {
            public MemoryMappedFile Mmf;
            public byte* Address;
            public long Size;
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

            var area = new MemoryMappedArea
            {
                Address = address,
                Mmf = mmf,
                Size = fs.Length
            };
            var mapping = new MemoryMapping();
            mapping.refCount = 1;
            mapping.fileStream = fs;
            mapping.baseAddress = address;
            mapping.areas.Add(area);
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
                   offsetPointer[1], offsetPointer[0], new UIntPtr(bytesToMap), null);
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
            if (Interlocked.Decrement(ref refCount) == 0)
            {
                foreach (var a in areas)
                {
                    Win32FileMapping.UnmapViewOfFile(a.Address);
                    a.Mmf.Dispose();
                }
            }
        }

        internal void AddRef()
        {
            Interlocked.Increment(ref refCount);
        }

        internal void Flush()
        {
            foreach (var area in areas)
            {
                if (!Win32FileMapping.FlushViewOfFile(area.Address, new IntPtr(area.Size)))
                {
                    throw new Win32Exception();
                }
            }
        }
    }
}
