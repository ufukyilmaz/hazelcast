package com.hazelcast.quorum.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.quorum.QuorumCompatibilityTest;

import static org.junit.Assert.assertEquals;

public class SetQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ISet<String> set = previousVersionMember.getSet(name);
        set.add("1");
        set.add("2");
        set.add("3");
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        ISet<String> setOnCurrentVersion = member.getSet(name);
        // no quorum applies while operating in 3.9 cluster version
        assertEquals(3, setOnCurrentVersion.size());
        for (int i = 10; i < 20; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        ISet<String> setOnCurrentVersion = member.getSet(name);
        setOnCurrentVersion.add("20");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        ISet<String> setOnCurrentVersion = member.getSet(name);
        for (int i = 20; i < 30; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }

        assertEquals(23, setOnCurrentVersion.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addSetConfig(new SetConfig(name).setQuorumName("pq"));
    }
}
