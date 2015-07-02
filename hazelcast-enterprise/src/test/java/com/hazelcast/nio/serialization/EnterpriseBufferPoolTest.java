package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.impl.EnterpriseBufferPool;
import com.hazelcast.nio.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseBufferPoolTest {

    private EnterpriseSerializationService serializationService;
    private EnterpriseBufferPool bufferPool;

    @Before
    public void setup() {
        serializationService = new EnterpriseSerializationServiceBuilder().build();
        bufferPool = new EnterpriseBufferPool(serializationService);
    }

    @Test
    public void returnInputBuffer_whenNull_thenIgnored(){
        bufferPool.returnInputBuffer(null);
    }

    @Test
    public void inputBufferIsNotPooled() {
        Data data = serializationService.toData("foo");
        BufferObjectDataInput in1 =  bufferPool.takeInputBuffer(data);
        bufferPool.returnInputBuffer(in1);

        BufferObjectDataInput in2 =  bufferPool.takeInputBuffer(data);
        assertNotSame(in1, in2);
    }
}
