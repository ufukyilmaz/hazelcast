package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractEnterpriseBufferObjectDataInputFactoryTest extends AbstractEnterpriseSerializationTest {

    protected InputOutputFactory factory;

    @Before
    public void setUp() {
        initMemoryManagerAndSerializationService();

        factory = getFactory();
    }

    protected abstract InputOutputFactory getFactory();

    protected abstract ByteOrder getByteOrder();

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testGetByteOrder() {
        assertEquals(getByteOrder(), factory.getByteOrder());
    }

    @Test
    public void testCreateInput_fromNativeData() {
        Data nativeData = serializationService.toData("TEST DATA", DataType.NATIVE);

        BufferObjectDataInput input = factory.createInput(nativeData, serializationService);

        assertNotNull(input);
        assertEquals(getByteOrder(), input.getByteOrder());
    }

    @Test
    public void testCreateInput_fromHeapData() {
        Data nativeData = serializationService.toData("TEST DATA", DataType.HEAP);

        BufferObjectDataInput input = factory.createInput(nativeData, serializationService);

        assertNotNull(input);
        assertEquals(getByteOrder(), input.getByteOrder());
    }

    @Test
    public void testCreateInput_fromByte() throws Exception {
        BufferObjectDataInput input = factory.createInput(getDefaultPayload(getByteOrder()), serializationService);

        assertNotNull(input);
        assertEquals(getByteOrder(), input.getByteOrder());
        assertDataLengthAndContent(getDefaultPayload(getByteOrder()),
                ((EnterpriseBufferObjectDataInput) input).readData(DataType.NATIVE));
    }

    @Test
    public void testCreateOutput() throws Exception {
        BufferObjectDataOutput output = factory.createOutput(1024, serializationService);

        assertNotNull(output);
        assertEquals(getByteOrder(), output.getByteOrder());

        output.close();
    }
}
