package com.hazelcast.quorum.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalMultiMapQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        TransactionContext context = getTransactionalContext(previousVersionMember);
        context.beginTransaction();
        TransactionalMultiMap<String, String> map = context.getMultiMap(name);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        assertEquals(3, map.size());
        context.commitTransaction();
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalMultiMap<String, String> map = context.getMultiMap(name);
        map.put("4", "d");
        assertEquals(4, map.size());
        context.commitTransaction();
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalMultiMap<String, String> map = context.getMultiMap(name);
        try {
            map.put("5", "e");
        } finally {
            context.rollbackTransaction();
        }
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalMultiMap<String, String> map = context.getMultiMap(name);
        map.put("6", "f");
        assertEquals(5, map.size());
        context.commitTransaction();
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addMultiMapConfig(
                new MultiMapConfig(name).setQuorumName("pq")
        );
    }
}
