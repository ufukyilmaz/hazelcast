package com.hazelcast.quorum.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.quorum.QuorumCompatibilityTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SemaphoreQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ISemaphore semaphore = previousVersionMember.getSemaphore(name);
        assertTrue(semaphore.init(2));
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        ISemaphore semaphore = member.getSemaphore(name);
        try {
            semaphore.acquire(1);
            semaphore.release();
            assertEquals(2, semaphore.availablePermits());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        ISemaphore semaphore = member.getSemaphore(name);
        semaphore.increasePermits(2);
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        ISemaphore semaphore = member.getSemaphore(name);
        semaphore.increasePermits(2);
        assertEquals(4, semaphore.availablePermits());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addSemaphoreConfig(new SemaphoreConfig()
                        .setName(name)
                        .setQuorumName("pq")
                );
    }
}
