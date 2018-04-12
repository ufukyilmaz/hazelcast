package com.hazelcast.quorum.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ReplicatedMapQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ReplicatedMap<String, String> map = previousVersionMember.getReplicatedMap(name);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        assertEquals(3, map.size());
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        ReplicatedMap<String, String> map = member.getReplicatedMap(name);
        map.put("4", "d");
        assertEquals(4, map.size());
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        ReplicatedMap<String, String> map = member.getReplicatedMap(name);
        map.put("5", "e");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        ReplicatedMap<String, String> map = member.getReplicatedMap(name);
        map.put("6", "f");
        assertEquals(5, map.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addReplicatedMapConfig(
                new ReplicatedMapConfig(name).setQuorumName("pq")
        );
    }
}
