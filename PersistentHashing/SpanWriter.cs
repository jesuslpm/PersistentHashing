using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{
    public ref struct SpanWriter
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

        public unsafe void Write<T>(T value) where T: unmanaged
        {
            if (_position + sizeof(T) > span.Length) throw new ArgumentException();
            var slice = span.Slice(_position);
            MemoryMarshal.Cast<byte, T>(slice)[0] = value;
            _position += sizeof(T);
        }

        public void Write(string value)
        {
            if (_position + sizeof(int) + value.Length * sizeof(char) > span.Length) throw new ArgumentException();

        }
    }
}
