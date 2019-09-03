package com.hazelcast.splitbrainprotection.collection;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ListSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {
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
    protected void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member) {
        IList<String> listOnCurrentVersion = member.getList(name);
        listOnCurrentVersion.add("20");
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member) {
        IList<String> listOnCurrentVersion = member.getList(name);
        for (int i = 20; i < 30; i++) {
            listOnCurrentVersion.add(Integer.toString(i));
            count++;
        }

        assertEquals(count, listOnCurrentVersion.size());
    }

    @Override
    protected Config getSplitBrainProtectedConfig() {
        return getConfigWithSplitBrainProtection().addListConfig(new ListConfig(name).setSplitBrainProtectionName("pq"));
    }
}
