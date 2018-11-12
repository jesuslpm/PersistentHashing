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
           
            CalculateOffsetsAndSizesDependingOnAlignement();
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

        private void CalculateOffsetsAndSizesDependingOnAlignement()
        {
            config.KeyOffset = 0;
            config.KeySize = sizeof(TKey);
            config.ValueSize = sizeof(long);
            config.DistanceSize = sizeof(short);


            int keyAlignement = GetAlignement(config.KeySize);
            int valueAlignement = GetAlignement(config.ValueSize);
            int distanceAlignement = GetAlignement(config.DistanceSize);
            int slotAlignement = Math.Max(distanceAlignement, Math.Max(keyAlignement, valueAlignement));

            //config.KeyOffset = 0;
            config.ValueOffset = config.KeyOffset + config.KeySize + GetPadding(config.KeyOffset + config.KeySize, valueAlignement);
            config.DistanceOffset = config.ValueOffset + config.ValueSize + GetPadding(config.ValueOffset + config.ValueSize, distanceAlignement);
            config.RecordSize = config.DistanceOffset + config.DistanceSize + GetPadding(config.DistanceOffset + config.DistanceSize, slotAlignement);
        }

        public StaticConcurrentFixedSizeHashTable<TKey, TValue> OpenThreadSafe()
        {
            config.IsThreadSafe = true;
            EnsureInitialized();
            if (!config.IsThreadSafe) throw new InvalidOperationException("This store is not thread safe");
            return new StaticConcurrentFixedSizeHashTable<TKey, TValue>(config);
        }
    }
}
