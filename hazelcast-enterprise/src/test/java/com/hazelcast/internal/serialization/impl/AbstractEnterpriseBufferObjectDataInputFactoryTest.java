package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
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

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testCreateInput_fromNativeData() {
        Data nativeData = serializationService.toData("TEST DATA", DataType.NATIVE);

        BufferObjectDataInput input = factory.createInput(nativeData, serializationService);

        assertNotNull(input);
        assertEquals(LITTLE_ENDIAN, input.getByteOrder());
    }

    @Test
    public void testCreateInput_fromHeapData() {
        Data nativeData = serializationService.toData("TEST DATA", DataType.HEAP);

        BufferObjectDataInput input = factory.createInput(nativeData, serializationService);

        assertNotNull(input);
        assertEquals(LITTLE_ENDIAN, input.getByteOrder());
    }

    @Test
    public void testCreateInput_fromByte() throws Exception {
        BufferObjectDataInput input = factory.createInput(DEFAULT_PAYLOAD, serializationService);

        assertNotNull(input);
        assertEquals(LITTLE_ENDIAN, input.getByteOrder());
        assertDataLengthAndContent(DEFAULT_PAYLOAD, ((EnterpriseBufferObjectDataInput) input).readData(DataType.NATIVE));
    }

    @Test
    public void testCreateOutput() throws Exception {
        BufferObjectDataOutput output = factory.createOutput(1024, serializationService);

        assertNotNull(output);
        assertEquals(LITTLE_ENDIAN, output.getByteOrder());

        output.close();
    }
}
