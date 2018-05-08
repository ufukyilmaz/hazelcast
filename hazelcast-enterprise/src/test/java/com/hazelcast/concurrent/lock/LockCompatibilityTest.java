package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.starter.HazelcastStarterUtils.assertInstanceOfByClassName;
import static org.junit.Assert.fail;

/**
 * Compatibility test for ILock.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class LockCompatibilityTest extends LockBasicTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        return createHazelcastInstanceFactory().newInstances();
    }

    @Override
    @Test(timeout = 60000)
    public void testDestroyLockWhenOtherWaitingOnLock() {
        try {
            super.testDestroyLockWhenOtherWaitingOnLock();
            fail("Expected DistributedObjectDestroyedException but no exeption was thrown.");
        } catch (Throwable t) {
            assertInstanceOfByClassName("com.hazelcast.spi.exception.DistributedObjectDestroyedException", t);
        }
    }
}
