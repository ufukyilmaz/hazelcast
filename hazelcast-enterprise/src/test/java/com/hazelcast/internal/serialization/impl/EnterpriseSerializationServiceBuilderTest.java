package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseSerializationServiceBuilderTest extends DefaultSerializationServiceBuilderTest {

    @Override
    protected SerializationServiceBuilder getSerializationServiceBuilder() {
        return new EnterpriseSerializationServiceBuilder();
    }
}
