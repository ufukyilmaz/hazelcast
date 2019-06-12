package com.hazelcast.quorum.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ReplicatedMapQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ReplicatedMap<String, String> map = previousVersionMember.getReplicatedMap(name);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        count = 3;
        assertEquals(count, map.size());
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        ReplicatedMap<String, String> map = member.getReplicatedMap(name);
        map.put("5", "e");
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        ReplicatedMap<String, String> map = member.getReplicatedMap(name);
        map.put(Integer.toString(++count), "f");
        assertEquals(count, map.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addReplicatedMapConfig(
                new ReplicatedMapConfig(name).setQuorumName("pq")
        );
    }
}
