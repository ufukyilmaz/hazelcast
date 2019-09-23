package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public abstract class AbstractEnterpriseBufferObjectDataOutputTest extends AbstractEnterpriseSerializationTest {

    private static final String TEST_STR = "TEST DATA";

    private EnterpriseBufferObjectDataOutput dataOutput;

    @Before
    public void setUp() {
        initMemoryManagerAndSerializationService();

        dataOutput = getEnterpriseByteArrayObjectDataOutput();
    }

    protected abstract EnterpriseBufferObjectDataOutput getEnterpriseByteArrayObjectDataOutput();

    @After
    public void tearDown() throws Exception {
        dataOutput.close();
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testWriteData_withNativeData() throws IOException {
        Data nativeData = serializationService.toData(TEST_STR, DataType.NATIVE);

        dataOutput.writeData(nativeData);

        byte[] bytes = dataOutput.toByteArray();
        assertEquals(bytes.length, nativeData.totalSize() + Bits.INT_SIZE_IN_BYTES);
    }

    @Test
    public void testWriteData_withHeapData() throws IOException {
        Data nativeData = serializationService.toData(TEST_STR, DataType.HEAP);

        dataOutput.writeData(nativeData);

        byte[] bytes = dataOutput.toByteArray();
        assertEquals(bytes.length, nativeData.totalSize() + Bits.INT_SIZE_IN_BYTES);
    }

    @Test
    public void testCopyFromMemoryBlock() throws IOException {
        Data nativeData = serializationService.toData(TEST_STR, DataType.NATIVE);

        dataOutput.copyFromMemoryBlock((MemoryBlock) nativeData, NativeMemoryData.PARTITION_HASH_OFFSET, nativeData.totalSize());

        byte[] bytes = dataOutput.toByteArray();
        assertEquals(bytes.length, nativeData.totalSize());
    }

    @Test(expected = IOException.class)
    public void testCopyFromMemoryBlock_shouldThrowIfLengthExceedsMemorySize() throws Exception {
        Data nativeData = serializationService.toData(TEST_STR, DataType.NATIVE);

        dataOutput.copyFromMemoryBlock((MemoryBlock) nativeData, NativeMemoryData.PARTITION_HASH_OFFSET,
                nativeData.totalSize() + 1024);
    }

    @Test(expected = IOException.class)
    public void testCopyToMemoryBlock_shouldThrowIfLengthExceedsSize() throws Exception {
        MemoryBlock memoryBlock = new NativeMemoryData(0, 512);

        dataOutput.copyToMemoryBlock(memoryBlock, 0, 1024);
    }

    @Test(expected = IOException.class)
    public void testCopyToMemoryBlock_shouldThrowIfLengthExceedsMemorySize() throws Exception {
        MemoryBlock memoryBlock = new NativeMemoryData(0, 512);

        dataOutput.copyToMemoryBlock(memoryBlock, 1024, 0);
    }

    @Test
    public void testGetSerializationService() throws Exception {
        assertEquals(serializationService, dataOutput.getSerializationService());
    }
}
