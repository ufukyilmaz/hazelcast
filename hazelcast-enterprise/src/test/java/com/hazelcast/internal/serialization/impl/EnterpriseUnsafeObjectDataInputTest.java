package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseUnsafeObjectDataInputTest extends AbstractEnterpriseBufferObjectDataInputTest {

    @Override
    protected EnterpriseBufferObjectDataInput getEnterpriseBufferObjectDataInput() {
        return getEnterpriseUnsafeObjectDataInput(getDefaultPayload(getByteOrder()));
    }

    @Override
    protected ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }
}
