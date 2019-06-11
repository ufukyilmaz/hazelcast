package com.hazelcast.quorum.atomiclong;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicLongQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        IAtomicLong atomicLong = previousVersionMember.getAtomicLong(name);
        assertEquals(++count, atomicLong.incrementAndGet());
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        IAtomicLong atomicLong = member.getAtomicLong(name);
        atomicLong.incrementAndGet();
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        IAtomicLong atomicLong = member.getAtomicLong(name);
        assertEquals(++count, atomicLong.incrementAndGet());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addAtomicLongConfig(new AtomicLongConfig(name).setQuorumName("pq"));
    }
}
