package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseByteArrayInputOutputFactoryTest extends AbstractEnterpriseBufferObjectDataInputFactoryTest {

    @Override
    protected InputOutputFactory getFactory() {
        return new EnterpriseByteArrayInputOutputFactory(LITTLE_ENDIAN);
    }

    @Test
    public void testGetByteOrder() {
        assertEquals(LITTLE_ENDIAN, factory.getByteOrder());
    }
}
