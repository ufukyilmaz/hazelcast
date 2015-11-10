package com.hazelcast.internal.serialization.impl;

import java.nio.ByteOrder;

import com.hazelcast.internal.serialization.SerializationService;

public class EnterpriseOffHeapObjectDataInput extends OffHeapByteArrayObjectDataInput {
    EnterpriseOffHeapObjectDataInput(long dataAddress, long dataSize, SerializationService service, ByteOrder byteOrder) {
        super(dataAddress, dataSize, service, byteOrder);
    }
}
