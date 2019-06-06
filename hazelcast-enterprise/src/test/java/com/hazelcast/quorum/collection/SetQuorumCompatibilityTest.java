package com.hazelcast.quorum.collection;

import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class SetQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        ISet<String> set = previousVersionMember.getSet(name);
        set.add("1");
        set.add("2");
        set.add("3");
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        ISet<String> setOnCurrentVersion = member.getSet(name);
        setOnCurrentVersion.add("20");
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        ISet<String> setOnCurrentVersion = member.getSet(name);
        for (int i = 20; i < 30; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }

        assertEquals(13, setOnCurrentVersion.size());
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addSetConfig(new SetConfig(name).setQuorumName("pq"));
    }
}
