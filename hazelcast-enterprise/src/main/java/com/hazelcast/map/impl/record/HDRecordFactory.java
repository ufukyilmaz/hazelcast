package com.hazelcast.map.impl.record;

import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Factory for creating Hi-Density backed records. Created for every
 * partition. The records will be created in Hi-Density memory storage.
 */
public class HDRecordFactory implements RecordFactory<Data> {

    private final HiDensityRecordProcessor<HDRecord> recordProcessor;
    private final EnterpriseSerializationService serializationService;
    private final HazelcastMemoryManager memoryManager;

    public HDRecordFactory(HiDensityRecordProcessor<HDRecord> recordProcessor,
                           SerializationService serializationService) {
        this.recordProcessor = recordProcessor;
        this.serializationService = ((EnterpriseSerializationService) serializationService);
        this.memoryManager = this.serializationService.getMemoryManager();
    }

    @Override
    public Record<Data> newRecord(Data key, Object value) {
        long address = NULL_PTR;
        Data dataValue = null;
        try {
            address = recordProcessor.allocate(HDRecord.SIZE);
            HDRecord record = recordProcessor.newRecord();
            record.reset(address);

            dataValue = recordProcessor.toData(value, DataType.NATIVE);
            record.setValue(dataValue);
            record.setLastAccessTime(UNSET);

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

    @Override
    public void setValue(Record<Data> record, Object value) {
        Data data = serializationService.toNativeData(value, memoryManager);
        record.setValue(data);
    }

    public HiDensityRecordProcessor<HDRecord> getRecordProcessor() {
        return recordProcessor;
    }

    static boolean isNull(Object object) {
        if (object == null) {
            return false;
        }

        NativeMemoryData memoryBlock = (NativeMemoryData) object;
        return memoryBlock.address() == NULL_PTR;
    }
}
