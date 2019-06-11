package com.hazelcast.quorum.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class MultiMapQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        MultiMap<String, String> map = previousVersionMember.getMultiMap(name);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        count = 3;
        assertEquals(count, map.size());
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        MultiMap<String, String> map = member.getMultiMap(name);
        map.put("5", "e");
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        MultiMap<String, String> map = member.getMultiMap(name);
        map.put(Integer.toString(++count), "f");
        assertEquals(count, map.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addMultiMapConfig(
                new MultiMapConfig(name).setQuorumName("pq")
        );
    }
}
