package com.hazelcast.quorum.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class MultiMapQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        MultiMap<String, String> map = previousVersionMember.getMultiMap(name);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        assertEquals(3, map.size());
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        MultiMap<String, String> map = member.getMultiMap(name);
        map.put("4", "d");
        assertEquals(4, map.size());
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        MultiMap<String, String> map = member.getMultiMap(name);
        map.put("5", "e");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        MultiMap<String, String> map = member.getMultiMap(name);
        map.put("6", "f");
        assertEquals(5, map.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addMultiMapConfig(
                new MultiMapConfig(name).setQuorumName("pq")
        );
    }
}
