package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.serialization.DataInputOutputTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseDataInputOutputTest extends DataInputOutputTest {

    @Override
    protected SerializationServiceBuilder createSerializationServiceBuilder() {
        return new EnterpriseSerializationServiceBuilder();
    }
}
