package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

public class HDRecordComparator implements RecordComparator {

    private final SerializationService serializationService;

    public HDRecordComparator(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public boolean isEqual(Object incomingValue, Object existingValue) {
        if (incomingValue == existingValue) {
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
}
