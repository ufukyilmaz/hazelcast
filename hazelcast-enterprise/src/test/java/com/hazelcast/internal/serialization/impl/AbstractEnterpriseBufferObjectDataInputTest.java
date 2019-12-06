package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractEnterpriseBufferObjectDataInputTest extends AbstractEnterpriseSerializationTest {

    private EnterpriseBufferObjectDataInput input;

    @Before
    public void setUp() throws Exception {
        initMemoryManagerAndSerializationService();

        input = getEnterpriseBufferObjectDataInput();
    }

    protected abstract EnterpriseBufferObjectDataInput getEnterpriseBufferObjectDataInput();

    protected abstract ByteOrder getByteOrder();

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test(expected = EOFException.class)
    public void testCopyToMemoryBlock_shouldThrowIfLengthExceedsSize() throws Exception {
        MemoryBlock memoryBlock = new NativeMemoryData(0, 512);

        input.copyToMemoryBlock(memoryBlock, 0, 1024);
    }

    @Test(expected = IOException.class)
    public void testCopyToMemoryBlock_shouldThrowIfLengthExceedsMemorySize() throws Exception {
        MemoryBlock memoryBlock = new NativeMemoryData(0, 512);

        input.copyToMemoryBlock(memoryBlock, 1024, 0);
    }

    @Test
    public void testGetSerializationService() throws Exception {
        assertEquals(serializationService, input.getSerializationService());
    }

    @Test
    public void testReadData() throws Exception {
        Data output = input.readData(DataType.NATIVE);

        assertNotNull(output);
        assertDataLengthAndContent(getDefaultPayload(getByteOrder()), output);
    }

    @Test
    public void testTryReadData() throws Exception {
        Data output = input.tryReadData(DataType.NATIVE);

        assertNotNull(output);
        assertDataLengthAndContent(getDefaultPayload(getByteOrder()), output);
    }
}
