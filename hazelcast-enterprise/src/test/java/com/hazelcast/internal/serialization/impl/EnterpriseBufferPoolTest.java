package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseBufferPoolTest extends AbstractEnterpriseSerializationTest {

    private EnterpriseBufferPool bufferPool;

    @Before
    public void setup() {
        initMemoryManagerAndSerializationService();
        bufferPool = new EnterpriseBufferPool(serializationService);
    }

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void returnInputBuffer_whenNull_thenIgnored() {
        bufferPool.returnInputBuffer(null);
    }

    @Test
    public void inputBufferIsNotPooled() {
        Data data = serializationService.toData("foo");
        BufferObjectDataInput in1 = bufferPool.takeInputBuffer(data);
        bufferPool.returnInputBuffer(in1);

        BufferObjectDataInput in2 = bufferPool.takeInputBuffer(data);
        assertNotSame(in1, in2);
    }
}
