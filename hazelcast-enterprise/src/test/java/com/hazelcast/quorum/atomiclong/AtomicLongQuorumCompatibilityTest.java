package com.hazelcast.quorum.atomiclong;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.quorum.QuorumCompatibilityTest;

import static org.junit.Assert.assertEquals;

public class AtomicLongQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        IAtomicLong atomicLong = previousVersionMember.getAtomicLong(name);
        assertEquals(1, atomicLong.incrementAndGet());
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        IAtomicLong atomicLong = member.getAtomicLong(name);
        assertEquals(2, atomicLong.incrementAndGet());
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        IAtomicLong atomicLong = member.getAtomicLong(name);
        atomicLong.incrementAndGet();
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        IAtomicLong atomicLong = member.getAtomicLong(name);
        assertEquals(3, atomicLong.incrementAndGet());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addAtomicLongConfig(new AtomicLongConfig(name).setQuorumName("pq"));
    }
}
