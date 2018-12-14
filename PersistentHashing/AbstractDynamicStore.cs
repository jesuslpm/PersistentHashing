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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace PersistentHashing
{
    public unsafe abstract class AbstractDynamicStore<TKey, TValue>: IDisposable
    {
        // <key><value-padding><value><distance padding><distance16><record-padding>
        internal DynamicHashTableSizeState sizeState;
        internal DynamicHashTableConfig<TKey, TValue> config;

        /// <summary>
        /// Max distance ever seen in the hash table. 
        /// MaxDistance is updated only on adding, It is not uptaded on removing.
        /// It' only updated when building with DEBUG simbol defined.
        /// </summary>
        // MaxDistance starts with 0 while internal distance starts with 1. So, it is the real max distance.
        public int MaxDistance => sizeState.HeaderPointer->MaxDistance;

        //Note that float casts are not redundant as VS says.
        public float LoadFactor => (float)Count / (float)sizeState.SlotCount;

        // this is only updated when building with DEBUG simbol defined.
        public float MeanDistance => (float)sizeState.HeaderPointer->DistanceSum / (float)Count;

        public long Capacity => sizeState.Capacity;

        public long Count => sizeState.HeaderPointer->RecordCount;

        public string HashTableFilePath => config.HashTableFilePath;
        public string DataFilePath => config.DataFilePath;


        private object initializeSyncObject = new object();
        protected volatile bool isInitialized;

       


        public AbstractDynamicStore(string filePathWithoutExtension, long initialCapacity, BaseHashTableOptions<TKey, TValue> options)
        {
            config.RecordSize = GetRecordSize();
            config.HashTableFilePath = filePathWithoutExtension + ".HashTable";
            config.DataFilePath = filePathWithoutExtension + ".DataFile";
            config.IsNew = !File.Exists(config.HashTableFilePath);

            if (options != null)
            {
                config.HashFunction = options.HashFunction;
                config.KeyComparer = options.KeyComparer ?? EqualityComparer<TKey>.Default;
                config.ValueComparer = options.ValueComparer ?? EqualityComparer<TValue>.Default;
            }

            sizeState = new DynamicHashTableSizeState();


            // SlotCount 1, 2, 4, and 8, 16 are edge cases that is not worth to support. So min slot count is 32, it doesn't show "anomalies".
            // we want a MaxLoadFactor = Capacity/SlotCount = 80% this is why we set SlotCount = capacity + capacity/4 => 4 * slotCount = 5 * capacity => capacity/SlotCount = 4/5 = 80%
            sizeState.SlotCount = Math.Max(initialCapacity + initialCapacity / 4, 32);
            sizeState.SlotCount = Bits.IsPowerOfTwo(sizeState.SlotCount) ? sizeState.SlotCount : Bits.NextPowerOf2(sizeState.SlotCount);
        }

        protected virtual void Initialize()
        {

            long initialFileSize = (long)sizeof(StaticHashTableFileHeader) + sizeState.TotalSlotCount * config.RecordSize;
            initialFileSize += (Constants.AllocationGranularity - (initialFileSize & Constants.AllocationGranularityMask)) & Constants.AllocationGranularityMask;
            config.TableMemoryMapper = new MemoryMapper(config.HashTableFilePath, initialFileSize);
            config.TableMappingSession = config.TableMemoryMapper.OpenSession();

            sizeState.TableFileBaseAddress = config.TableMappingSession.GetBaseAddress();
            sizeState.HeaderPointer = (StaticHashTableFileHeader*)sizeState.TableFileBaseAddress;

            if (config.IsNew) InitializeHeader();
            else
            {
                ValidateHeader();
                sizeState.SlotCount = sizeState.HeaderPointer->SlotCount;
            }
            sizeState.TablePointer = sizeState.TableFileBaseAddress + sizeof(StaticHashTableFileHeader);
            sizeState.EndTablePointer = sizeState.TablePointer + config.RecordSize * sizeState.TotalSlotCount;
            sizeState.HashMask = sizeState.SlotCount - 1;
            sizeState.SlotBits = Bits.MostSignificantBit(sizeState.SlotCount);
            sizeState.OverflowAreaSlotCount = (int)Math.Min(sizeState.SlotCount, 256);
            sizeState.TotalSlotCount = sizeState.SlotCount + sizeState.OverflowAreaSlotCount;
            sizeState.Capacity = sizeState.SlotCount / 5 * 4;

            config.DataFile = OpenDataFile();

        }

        protected abstract int GetRecordSize();

        protected abstract DataFile OpenDataFile();


        protected void InitializeHeader()
        {
            sizeState.HeaderPointer->DistanceSum = 0;
            sizeState.HeaderPointer->Magic = StaticHashTableFileHeader.MagicNumber;
            sizeState.HeaderPointer->MaxDistance = 0;
            sizeState.HeaderPointer->RecordCount = 0;
            sizeState.HeaderPointer->RecordSize = config.RecordSize;
            sizeState.HeaderPointer->SlotCount = sizeState.SlotCount;
        }

        protected void ValidateHeader()
        {
            if ( sizeState.HeaderPointer->Magic != StaticHashTableFileHeader.MagicNumber)
            {
                throw new FormatException($"This is not a {nameof(AbstractDynamicStore<TKey, TValue>)} file");
            }
            if (sizeState.HeaderPointer->RecordSize != config.RecordSize)
            {
                throw new ArgumentException("Mismatched RecordSize");
            }
        }


        public void WarmUp()
        {
            config.TableMappingSession.WarmUp();
        }


        public bool IsDisposed { get; private set; }

 
        public virtual void Dispose()
        {
            if (IsDisposed) return;
            IsDisposed = true;
            if (config.TableMappingSession != null) config.TableMappingSession.Dispose();
            if (config.TableMemoryMapper != null) config.TableMemoryMapper.Dispose();
            if (config.DataFile != null) config.DataFile.Dispose();
        }

        public virtual void Flush()
        {
            config.TableMemoryMapper.Flush();
            if (config.DataFile != null) config.DataFile.Flush();
        }

        public void EnsureInitialized()
        {
            if (isInitialized)
            {
                EnsureSyncObjectsForConcurrentHashTables();
            };
            lock (initializeSyncObject)
            {
                if (isInitialized)
                {
                    EnsureSyncObjectsForConcurrentHashTables();
                    return;
                }
                Initialize();
                EnsureSyncObjectsForConcurrentHashTables();
                isInitialized = true;
            }
        }

        private void EnsureSyncObjectsForConcurrentHashTables()
        {
            //if (config.IsThreadSafe && config.SyncObjects == null)
            //{
            //    lock (initializeSyncObject)
            //    {
            //        if (config.IsThreadSafe && config.SyncObjects == null)
            //        {
            //            config.SyncObjects = new SyncObject[config.ChunkCount];
            //            for (int i = 0; i < config.ChunkCount; i++) config.SyncObjects[i] = new SyncObject();
            //        }
            //    }
            //}
        }
    }
}
