package com.hazelcast.concurrent.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for Semaphore.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class SemaphoreCompatibilityTest extends SemaphoreBasicTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        return createHazelcastInstanceFactory().newInstances();
    }
}
