package com.hazelcast.internal.serialization.impl;

import java.nio.ByteOrder;

import com.hazelcast.nio.serialization.EnterpriseSerializationService;

public class EnterpriseOffHeapObjectDataOutput extends OffHeapByteArrayObjectDataOutput {
    EnterpriseOffHeapObjectDataOutput(long size, EnterpriseSerializationService service) {
        super(size, service, ByteOrder.BIG_ENDIAN);
    }
}
