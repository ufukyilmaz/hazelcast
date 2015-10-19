/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.record;

import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.hidensity.HiDensityRecordStore.NULL_PTR;

/**
 * Factory for creating hi-density backed records. Created for every partition.
 * The records will be created in hi-density memory storage.
 */
public class HDRecordFactory implements RecordFactory<Data> {

    private final HiDensityRecordProcessor<HDRecord> recordProcessor;
    private final EnterpriseSerializationService serializationService;
    private final boolean statisticsEnabled;
    private final MemoryManager memoryManager;

    public HDRecordFactory(HiDensityRecordProcessor<HDRecord> recordProcessor,
                           SerializationService serializationService,
                           boolean statisticsEnabled) {
        this.recordProcessor = recordProcessor;
        this.serializationService = ((EnterpriseSerializationService) serializationService);
        this.statisticsEnabled = statisticsEnabled;
        this.memoryManager = this.serializationService.getMemoryManager();
    }

    @Override
    public Record<Data> newRecord(Object value) {
        int size = statisticsEnabled ? StatsAwareHDRecord.SIZE : SimpleHDRecord.SIZE;

        long address = NULL_PTR;
        Data dataValue = null;
        try {
            address = recordProcessor.allocate(size);
            HDRecord record = recordProcessor.newRecord();
            record.reset(address);

            dataValue = recordProcessor.toData(value, DataType.NATIVE);
            record.setValue(dataValue);
            return record;

        } catch (NativeOutOfMemoryError error) {
            if (!isNull(dataValue)) {
                recordProcessor.disposeData(dataValue);
            }

            if (address != NULL_PTR) {
                recordProcessor.dispose(address);
            }

            throw error;
        }
    }


    private static boolean isNull(Object object) {
        if (object == null) {
            return false;
        }

        NativeMemoryData memoryBlock = (NativeMemoryData) object;
        return memoryBlock.address() == NULL_PTR;
    }

    @Override
    public void setValue(Record<Data> record, Object value) {
        Data data = serializationService.toNativeData(value, memoryManager);
        record.setValue(data);
    }

    @Override
    public boolean isEquals(Object incomingValue, Object existingValue) {
        if (incomingValue == null && existingValue == null) {
            return true;
        }
        if (incomingValue == null || existingValue == null) {
            return false;
        }

        if (!(incomingValue instanceof Data)) {
            incomingValue = serializationService.toData(incomingValue);
        }

        if (!(existingValue instanceof Data)) {
            existingValue = serializationService.toData(existingValue);
        }

        if (existingValue instanceof NativeMemoryData) {
            return NativeMemoryDataUtil.equals(((NativeMemoryData) existingValue).address(), ((Data) incomingValue));
        } else {
            return existingValue.equals(incomingValue);
        }

    }

    public HiDensityRecordProcessor<HDRecord> getRecordProcessor() {
        return recordProcessor;
    }
}
