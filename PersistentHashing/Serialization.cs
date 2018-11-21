using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace PersistentHashing
{

    public unsafe interface IValueSerializer<T>
    {
        T Deserialize(ReadOnlySpan<byte> source);
        long Serialize(T value, DataFile dataFile);
    }

    public unsafe interface ItemSerializer<TKey, TValue>
    {
        TKey DeserializeKey(ReadOnlySpan<byte> source);
        TValue DeserializeValue(ReadOnlySpan<byte> source);
        long Serialize(TKey key, TValue value, DataFile dataFile);
    }

    public unsafe class StringStringSerializer : ItemSerializer<string, string>
    {
        public string DeserializeKey(ReadOnlySpan<byte> source)
        {
            return MemoryMarshal.Cast<byte, char>(source).ToString();
        }

        public string DeserializeValue(ReadOnlySpan<byte> source)
        {
            return MemoryMarshal.Cast<byte, char>(source).ToString();
        }

        public long Serialize(string key, string value, DataFile dataFile)
        {
            var item = dataFile.AllocateItem(key.Length * sizeof(char), value.Length * sizeof(char));
            key.AsSpan().CopyTo(MemoryMarshal.Cast<byte, char>(item.KeySpan));
            value.AsSpan().CopyTo(MemoryMarshal.Cast<byte, char>(item.ValueSpan));
            return item.Offset;
        }
    }


    public unsafe class StringSerializer : IValueSerializer<string>
    {

        public string Deserialize(ReadOnlySpan<byte> source)
        {
            return MemoryMarshal.Cast<byte, char>(source).ToString();
        }

        public long Serialize(string value, DataFile dataFile)
        {
            var fileSlice = dataFile.AllocateValue(value.Length * sizeof(char));
            value.AsSpan().CopyTo(MemoryMarshal.Cast<byte, char>(fileSlice.Span));
            return fileSlice.Offset;
        }
    }

    public unsafe class Utf8Serializer: IValueSerializer<string>
    {

        private static readonly ArrayPool<byte> pool = ArrayPool<byte>.Shared;

        public string Deserialize(ReadOnlySpan<byte> source)
        {
            fixed (byte* buffer = source)
            {
                return Encoding.UTF8.GetString(buffer, source.Length);
            }
        }

        public long Serialize(string str, DataFile dataFile)
        {
            var size = Encoding.UTF8.GetByteCount(str);
            var fileSlice = dataFile.AllocateValue(size);
            fixed (byte* buffer = fileSlice.Span)
            fixed (char* chars = str)
            {
                Encoding.UTF8.GetBytes(chars, str.Length, buffer, size);
            }
            return fileSlice.Offset;
        }

    }
}
