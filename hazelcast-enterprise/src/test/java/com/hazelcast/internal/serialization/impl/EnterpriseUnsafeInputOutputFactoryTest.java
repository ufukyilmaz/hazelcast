package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseUnsafeInputOutputFactoryTest extends AbstractEnterpriseBufferObjectDataInputFactoryTest {

    @Override
    protected InputOutputFactory getFactory() {
        return new EnterpriseUnsafeInputOutputFactory();
    }

    @Test
    public void testGetByteOrder() {
        assertEquals(ByteOrder.nativeOrder(), factory.getByteOrder());
    }
}
