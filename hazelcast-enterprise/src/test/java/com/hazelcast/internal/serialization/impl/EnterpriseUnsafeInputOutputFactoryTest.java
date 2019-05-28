package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseUnsafeInputOutputFactoryTest extends AbstractEnterpriseBufferObjectDataInputFactoryTest {

    @Override
    protected InputOutputFactory getFactory() {
        return new EnterpriseUnsafeInputOutputFactory();
    }

    @Override
    protected ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }
}
