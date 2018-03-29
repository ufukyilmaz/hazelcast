package com.hazelcast.quorum.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.quorum.QuorumCompatibilityTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CountDownLatchQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ICountDownLatch latch = previousVersionMember.getCountDownLatch(name);
        assertTrue(latch.trySetCount(2));
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        ICountDownLatch latch = member.getCountDownLatch(name);
        latch.countDown();
        assertEquals(1, latch.getCount());
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        ICountDownLatch latch = member.getCountDownLatch(name);
        latch.countDown();
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        ICountDownLatch latch = member.getCountDownLatch(name);
        latch.countDown();
        assertOpenEventually(latch);
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addCountDownLatchConfig(new CountDownLatchConfig(name).setQuorumName("pq"));
    }
}
