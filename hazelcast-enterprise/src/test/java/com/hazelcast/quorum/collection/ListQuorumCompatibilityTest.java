package com.hazelcast.quorum.collection;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ListQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {
    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        IList<String> set = previousVersionMember.getList(name);
        set.add("1");
        set.add("2");
        set.add("3");
        count = 3;
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        IList<String> listOnCurrentVersion = member.getList(name);
        listOnCurrentVersion.add("20");
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        IList<String> listOnCurrentVersion = member.getList(name);
        for (int i = 20; i < 30; i++) {
            listOnCurrentVersion.add(Integer.toString(i));
            count++;
        }

        assertEquals(count, listOnCurrentVersion.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addListConfig(new ListConfig(name).setQuorumName("pq"));
    }
}
