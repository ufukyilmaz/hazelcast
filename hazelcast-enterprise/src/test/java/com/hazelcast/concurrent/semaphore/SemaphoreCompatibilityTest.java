package com.hazelcast.concurrent.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for Semaphore.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class SemaphoreCompatibilityTest extends SemaphoreBasicTest {

    // RU_COMPAT_3_9
    @Ignore("Operation increase permits was added in 3.10")
    @Test
    @Override
    public void testIncreasePermits() {
    }

    // RU_COMPAT_3_9
    @Ignore("Operation increase permits was added in 3.10")
    @Test
    @Override
    public void testIncrease_whenArgumentNegative() {
    }

    @Override
    protected HazelcastInstance[] newInstances() {
        return createHazelcastInstanceFactory().newInstances();
    }
}
