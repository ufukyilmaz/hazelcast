package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseByteArrayObjectDataInputTest extends AbstractEnterpriseBufferObjectDataInputTest {

    @Override
    protected EnterpriseBufferObjectDataInput getEnterpriseBufferObjectDataInput() {
        return getEnterpriseObjectDataInput(getDefaultPayload(getByteOrder()));
    }

    @Override
    protected ByteOrder getByteOrder() {
        return ByteOrder.LITTLE_ENDIAN;
    }
}