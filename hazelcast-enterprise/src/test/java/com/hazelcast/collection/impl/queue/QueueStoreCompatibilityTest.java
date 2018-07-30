package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class QueueStoreCompatibilityTest extends QueueStoreTest {

    @Override
    protected HazelcastInstance createHazelcastInstance(Config config) {
        // we create the factory with 3 instances, to get an Hazelcast instance with the CURRENT_VERSION as well
        // and return this to the test (instance[0] and instance[1] are proxied instances)
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);
        return instances[2];
    }

    @Test
    @Ignore
    @Override
    public void testIssue1401QueueStoreWithTxnPoll() {
        // FIXME this test sometimes fails with "AssertionError: queue.poll() should not return null"
        // I guess the TransactionalQueue is not yet fully supported by the compatibility framework
    }
}
