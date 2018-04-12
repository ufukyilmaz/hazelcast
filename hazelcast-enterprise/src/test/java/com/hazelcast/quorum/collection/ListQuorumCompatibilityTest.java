package com.hazelcast.quorum.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ListQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        IList<String> set = previousVersionMember.getList(name);
        set.add("1");
        set.add("2");
        set.add("3");
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        IList<String> setOnCurrentVersion = member.getList(name);
        // no quorum applies while operating in 3.9 cluster version
        assertEquals(3, setOnCurrentVersion.size());
        for (int i = 10; i < 20; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        IList<String> setOnCurrentVersion = member.getList(name);
        setOnCurrentVersion.add("20");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        IList<String> setOnCurrentVersion = member.getList(name);
        for (int i = 20; i < 30; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }

        assertEquals(23, setOnCurrentVersion.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addListConfig(new ListConfig(name).setQuorumName("pq"));
    }
}
