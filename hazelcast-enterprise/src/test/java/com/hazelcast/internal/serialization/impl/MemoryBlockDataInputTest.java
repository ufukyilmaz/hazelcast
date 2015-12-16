package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemoryBlockDataInputTest extends AbstractEnterpriseSerializationTest {

    private EnterpriseBufferObjectDataInput input;

    @Before
    public void setUp() throws Exception {
        initMemoryManagerAndSerializationService();

        Data nativeData = serializationService.toData("TEST DATA", DataType.NATIVE);
        EnterpriseUnsafeInputOutputFactory factory = new EnterpriseUnsafeInputOutputFactory();
        input = factory.createInput(nativeData, serializationService);
    }

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testGetSerializationService() throws Exception {
        assertEquals(serializationService, input.getSerializationService());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCopyToMemoryBlock() throws Exception {
        input.copyToMemoryBlock(null, 0, 0);
    }

    @Test
    public void testGetClassLoader() {
        assertEquals(serializationService.getClassLoader(), input.getClassLoader());
    }

    @Test
    public void testToString() {
        assertNotNull(input.toString());
    }
}
