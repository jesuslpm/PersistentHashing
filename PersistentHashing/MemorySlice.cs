using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    /// <summary>
    /// Represent a portion of memory
    /// </summary>
    /// <remarks>
    /// We need this because ReadOnlySpan cannot be used as generic type parameter
    /// </remarks>
    public unsafe struct MemorySlice
    {
        public void* Pointer;
        public int Size;

        public MemorySlice(void* pointer, int size)
        {
            this.Pointer = pointer;
            this.Size = size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> ToReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(Pointer, Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> ToSpan()
        {
            return new Span<byte>(Pointer, Size);
        }

        public static explicit operator ReadOnlySpan<byte>(MemorySlice slice)
        {
            return slice.ToReadOnlySpan();
        }

        public static explicit operator Span<byte>(MemorySlice slice)
        {
            return slice.ToSpan();
        }

        public static readonly MemorySliceEqualityComparer EqualityComparer = new MemorySliceEqualityComparer();
    }

    public class MemorySliceEqualityComparer : IEqualityComparer<MemorySlice>
    {
        public bool Equals(MemorySlice x, MemorySlice y)
        {
            return x.ToReadOnlySpan().SequenceEqual(y.ToReadOnlySpan());
        }

        public int GetHashCode(MemorySlice obj)
        {
            throw new NotImplementedException();
        }
    }
}
