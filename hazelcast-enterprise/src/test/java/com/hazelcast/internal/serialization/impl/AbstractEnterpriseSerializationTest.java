package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastTestSupport;

import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractEnterpriseSerializationTest extends HazelcastTestSupport {

    protected static final byte[] DEFAULT_PAYLOAD_LE = new byte[]{8, 0, 0, 0, 23, 42, 47, 11, 0, 8, 1, 5};
    protected static final byte[] DEFAULT_PAYLOAD_BE = new byte[]{0, 0, 0, 8, 23, 42, 47, 11, 0, 8, 1, 5};
    protected static final byte[] NULL_ARRAY_LENGTH_PAYLOAD = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

    protected HazelcastMemoryManager outOfMemoryMemoryManager;
    protected HazelcastMemoryManager memoryManager;

    protected EnterpriseSerializationService serializationService;

    protected void initMemoryManagerAndSerializationService() {
        outOfMemoryMemoryManager = mock(HazelcastMemoryManager.class);
        when(outOfMemoryMemoryManager.allocate(anyLong())).thenThrow(new NativeOutOfMemoryError());

        memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        serializationService = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(isUseNativeByteOrder())
                .build();
    }

    protected byte[] getDefaultPayload(ByteOrder byteOrder) {
        if (byteOrder == LITTLE_ENDIAN) {
            return DEFAULT_PAYLOAD_LE;
        } else {
            return DEFAULT_PAYLOAD_BE;
        }
    }

    protected boolean isUseNativeByteOrder() {
        return false;
    }

    protected void shutdownMemoryManagerAndSerializationService() {
        serializationService.dispose();
        memoryManager.dispose();
    }

    protected EnterpriseByteArrayObjectDataInput getEnterpriseObjectDataInput(byte[] payload) {
        return new EnterpriseByteArrayObjectDataInput(payload, 0, serializationService, LITTLE_ENDIAN);
    }

    protected EnterpriseUnsafeObjectDataInput getEnterpriseUnsafeObjectDataInput(byte[] payload) {
        return new EnterpriseUnsafeObjectDataInput(payload, 0, serializationService);
    }

    protected static void assertDataLengthAndContent(byte[] expectedBytes, Data data) {
        int expectedSize = expectedBytes.length - INT_SIZE_IN_BYTES;
        assertEquals(expectedSize, data.totalSize());

        byte[] dataByteArray = data.toByteArray();
        for (int i = 0; i < expectedSize; i++) {
            assertEquals(expectedBytes[i + INT_SIZE_IN_BYTES], dataByteArray[i]);
        }
    }
}
