using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public unsafe sealed class StaticFixedSizeStore<TKey, TValue>: StaticStore<TKey, TValue>, IDisposable 
        where TKey:unmanaged 
        where TValue: unmanaged
    {


        public StaticFixedSizeStore(string filePathPathWithoutExtension, long capacity, HashTableOptions<TKey, TValue> options = null)
            :base(filePathPathWithoutExtension, capacity, options)
        {
        }

        public override void Initialize()
        {
           
            long fileSize = (long)sizeof(StaticFixedSizeHashTableFileHeader) + config.SlotCount * config.RecordSize;
            fileSize += (Constants.AllocationGranularity - (fileSize & Constants.AllocationGranularityMask)) & Constants.AllocationGranularityMask;
            config.TableMemoryMapper = new MemoryMapper(config.HashTableFilePath, fileSize);
            config.TableMappingSession = config.TableMemoryMapper.OpenSession();

            config.TableFileBaseAddress = config.TableMappingSession.GetBaseAddress();
            config.HeaderPointer = (StaticFixedSizeHashTableFileHeader*)config.TableFileBaseAddress;

            if (config.IsNew) InitializeHeader();
            else
            {
                ValidateHeader();
                config.SlotCount = config.HeaderPointer->SlotCount;
            }
            config.TablePointer = config.TableFileBaseAddress + sizeof(StaticFixedSizeHashTableFileHeader);
            config.EndTablePointer = config.TablePointer + config.RecordSize * config.SlotCount;
        }

        protected override int GetRecordSize()
        {
            return Unsafe.SizeOf<StaticHashTableRecord<TKey, TValue>>();
        }

        StaticConcurrentFixedSizeHashTable<TKey, TValue> OpenThreadSafe()
        {
            EnsureInitialized();
            if (!config.IsThreadSafe) throw new InvalidOperationException("This store is not thread safe");
            return new StaticConcurrentFixedSizeHashTable<TKey, TValue>(config);
        }
    }
}
