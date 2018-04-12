package com.hazelcast.quorum.atomiclong;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicLongQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

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
