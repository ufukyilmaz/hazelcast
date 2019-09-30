package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.nio.ByteOrder.BIG_ENDIAN;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseByteArrayObjectDataOutputTest extends AbstractEnterpriseBufferObjectDataOutputTest {

    @Override
    protected EnterpriseBufferObjectDataOutput getEnterpriseByteArrayObjectDataOutput() {
        return new EnterpriseByteArrayObjectDataOutput(2, serializationService, BIG_ENDIAN);
    }
}
