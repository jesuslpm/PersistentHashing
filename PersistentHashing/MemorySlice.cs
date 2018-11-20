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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        public T[] ToArray<T>() where T:unmanaged
        {
            int length = (Size + sizeof(T) - 1) / sizeof(T);
            var a = new T[length];
            if (length > 0) a[length - 1] = default;
            ToReadOnlySpan().CopyTo(MemoryMarshal.Cast<T, byte>(a.AsSpan<T>()));
            return a;
        }

        public static explicit operator ReadOnlySpan<byte>(MemorySlice slice)
        {
            return slice.ToReadOnlySpan();
        }

        public static explicit operator Span<byte>(MemorySlice slice)
        {
            return slice.ToSpan();
        }

        public bool SequenceEquals<TItem>(TItem[] buffer) where TItem: unmanaged
        {
            return this.ToReadOnlySpan().SequenceEqual(MemoryMarshal.Cast<TItem, byte>(buffer.AsSpan()));
        }

        public bool SequenceEquals(MemorySlice other)
        {
            return this.ToReadOnlySpan().SequenceEqual(other.ToReadOnlySpan());
        }

        public bool SequenceEquals<TItem>(ReadOnlySpan<TItem> span) where TItem: unmanaged
        {
            return this.ToReadOnlySpan().SequenceEqual(MemoryMarshal.Cast<TItem, byte>(span));
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
