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

    public unsafe struct SerializationTarget
    {
        private DataFile dataFile;
        private byte* baseAddress;
        internal long dataOffset;
        internal int size;

        public SerializationTarget(DataFile dataFile, byte* baseAddress)
        {
            this.dataFile = dataFile;
            this.dataOffset = 0;
            this.baseAddress = baseAddress;
            this.size = 0;
        }

        public void* GetTargetAddress(int size)
        {
            if (dataOffset == 0)
            {
                this.size = size;
                dataOffset = dataFile.Allocate(size);
            }
            else if (size != this.size)
            {
                throw new InvalidOperationException("Allocating is a one time operation");
            }
            return baseAddress + dataOffset;
        }
    }


    public unsafe interface IValueSerializer<T>
    {
        T Deserialize(void* source);
        void Serialize(T obj, SerializationTarget target);
    }

    public unsafe interface ItemSerializer<TKey, TValue>
    {
        TKey DeserializeKey(void* source);
        TValue DeserializeValue(void* source);
        void Serialize(TKey key, TValue value, SerializationTarget target);
    }


    public unsafe class StringSerializer : IValueSerializer<string>
    {
        public string Deserialize(void* source)
        {
            return new string((char*)((int *)source + 1), 0, *(int*) source);
        }

        public void Serialize(string obj, SerializationTarget serializationTarget)
        {
            fixed (void *pointer = obj)
            {
                void* targetAddress = serializationTarget.GetTargetAddress(obj.Length * sizeof(char) + sizeof(int));
                *(int*)targetAddress = obj.Length;
                var targetSpan = new Span<byte>((int*)targetAddress + 1, obj.Length * sizeof(char));
                var sourceSpan = new ReadOnlySpan<byte>(pointer, obj.Length * sizeof(char));
                sourceSpan.CopyTo(targetSpan);
            }
        }
    }

    public unsafe class Utf8Serializer: IValueSerializer<string>
    {

        private static readonly ArrayPool<byte> pool = ArrayPool<byte>.Shared;

        public string Deserialize(void *source)
        {
            return Encoding.UTF8.GetString((byte*)((int *)source+1), *(int *)source / sizeof(char));
        }

        public void Serialize(string str, SerializationTarget target)
        {
            var size = str.Length * 4;
            if (size > 32 * 1024)
            {
                SerializeUsingArrayPool(str, target, size);
            }
            else
            {
                SerializeUsingStackAllocation(str, target, size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SerializeUsingArrayPool(string str, SerializationTarget target, int size)
        {
            byte[] buffer = null;
            try
            {
                buffer = pool.Rent(size);
                int byteCount = Encoding.UTF8.GetBytes(str, 0, str.Length, buffer, 0);
                var targetAddress = target.GetTargetAddress(byteCount + sizeof(int));
                *(int*)targetAddress = byteCount;
                var targetSpan = new Span<byte>((int*)targetAddress + 1, byteCount);
                buffer.AsSpan(0, byteCount).CopyTo(targetSpan);
            }
            finally
            {
                pool.Return(buffer);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SerializeUsingStackAllocation(string str, SerializationTarget target, int size)
        {
            // avoid declaring other local vars, or doing work with stackalloc
            // to prevent the .locals init cil flag , see: https://github.com/dotnet/coreclr/issues/1279
            byte* buffer = stackalloc byte[size];
            SerializeUsingStackAllocationImpl(buffer, str, target, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SerializeUsingStackAllocationImpl(byte* buffer, string str, SerializationTarget target, int size)
        {
            fixed (char* chars = str)
            {
                int byteCount = Encoding.UTF8.GetBytes(chars, str.Length, buffer, size);
                var targetAddress = target.GetTargetAddress(byteCount + sizeof(int));
                *(int*)targetAddress = byteCount;
                var targetSpan = new Span<byte>((int*)targetAddress + 1, byteCount);
                new Span<byte>(buffer, byteCount).CopyTo(targetSpan);
            } 
        }
    }
}
