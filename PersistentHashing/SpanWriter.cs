using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public unsafe ref struct SpanWriter
    {
        private Span<byte> span;
        private int _position;

        public int Position
        {
            get
            {
                return _position;
            }
            set
            {
                if (value > span.Length) throw new ArgumentException();
                _position = value;
            }
        }

        public SpanWriter(Span<byte> span)
        {
            this.span = span;
            _position = 0;
        }

        public void Write<T>(T value) where T: unmanaged
        {
            if (_position + sizeof(T) > span.Length) throw new ArgumentException();
            Unsafe.As<byte, T>(ref Unsafe.Add(ref span[0], _position)) = value;
            _position += sizeof(T);
        }

        public void Write<T>(T? value) where T : unmanaged
        {
            if (value==null)
            {
                Write(true);
            }
            else
            {
                Write(false);
                Write(value.Value);
            }
        }

        public void WriteUtf8(string value)
        {
            int bytesToWrite = sizeof(int) + (value == null ? 0 : Encoding.UTF8.GetByteCount(value));
            if (_position + bytesToWrite > span.Length) throw new ArgumentException();
            if (value == null)
            {
                Unsafe.As<byte, int>(ref Unsafe.Add(ref span[0], _position)) = -1;
            }
            else
            {
                var slice = span.Slice(_position + sizeof(int));
                fixed (byte* pointer = slice)
                fixed (char* chars = value)
                {
                    Encoding.UTF8.GetBytes(chars, value.Length, pointer, slice.Length);
                }
            }
            _position += bytesToWrite;
        }

        public void Write(string value)
        {
            int bytesToWrite = sizeof(int) + (value == null ? 0 : value.Length * sizeof(char));
            if (_position + bytesToWrite > span.Length) throw new ArgumentException();
            if (value == null)
            {
                Unsafe.As<byte, int>(ref Unsafe.Add(ref span[0], _position)) = -1;
            }
            else
            {
                Unsafe.As<byte, int>(ref Unsafe.Add(ref span[0], _position)) = value.Length;
                var destination = MemoryMarshal.Cast<byte, char>(span.Slice(_position + sizeof(int)));
                value.AsSpan().CopyTo(destination);
            }
            _position += bytesToWrite;
        }

        public void Write(ReadOnlySpan<byte> value)
        {
            if (_position + span.Length + sizeof(int) > span.Length) throw new ArgumentException();
            Unsafe.As<byte, int>(ref Unsafe.Add(ref span[0], _position)) = value.Length;
            value.CopyTo(span.Slice(_position));
            _position += span.Length + sizeof(int);
        }

        public void Write(byte[] value)
        {
            if (value == null)
            {
                Unsafe.As<byte, int>(ref Unsafe.Add(ref span[0], _position)) = -1;
                _position += sizeof(int);
            }
            else
            {
                Write(value.AsSpan());
            }
        }

        public void Write(ArraySegment<byte> value)
        {
            if (value == null)
            {
                Unsafe.As<byte, int>(ref Unsafe.Add(ref span[0], _position)) = -1;
                _position += sizeof(int);
            }
            else
            {
                Write(value.AsSpan());
            }
        }
    }
}
