package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount;

/**
 * Compatibility test for Lock
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class LockCompatibilityTest extends LockBasicTest {

    @Override
    protected HazelcastInstance[] newInstances() {
        return createHazelcastInstanceFactory().newInstances();
    }

}
