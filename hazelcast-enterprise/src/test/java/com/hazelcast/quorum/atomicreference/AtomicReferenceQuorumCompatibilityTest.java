package com.hazelcast.quorum.atomicreference;

import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class AtomicReferenceQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        IAtomicReference<String> atomicString = previousVersionMember.getAtomicReference(name);
        atomicString.set("1");
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        IAtomicReference<String> atomicString = member.getAtomicReference(name);
        assertTrue(atomicString.compareAndSet("1", "2"));
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        IAtomicReference<String> atomicString = member.getAtomicReference(name);
        atomicString.set("3");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        IAtomicReference<String> atomicString = member.getAtomicReference(name);
        assertTrue(atomicString.compareAndSet("2", "3"));
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addAtomicReferenceConfig(new AtomicReferenceConfig(name).setQuorumName("pq"));
    }
}
