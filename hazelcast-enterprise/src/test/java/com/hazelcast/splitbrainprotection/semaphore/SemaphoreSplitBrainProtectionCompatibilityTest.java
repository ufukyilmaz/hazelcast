package com.hazelcast.splitbrainprotection.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class SemaphoreSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ISemaphore semaphore = previousVersionMember.getSemaphore(name);
        assertTrue(semaphore.init(2));
        count = 2;
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member) {
        ISemaphore semaphore = member.getSemaphore(name);
        semaphore.increasePermits(2);
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member) {
        ISemaphore semaphore = member.getSemaphore(name);
        semaphore.increasePermits(1);
        assertEquals(++count, semaphore.availablePermits());
    }

    @Override
    protected Config getSplitBrainProtectedConfig() {
        return getConfigWithSplitBrainProtection()
                .addSemaphoreConfig(new SemaphoreConfig()
                        .setName(name)
                        .setSplitBrainProtectionName("pq")
                );
    }
}
