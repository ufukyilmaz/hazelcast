package com.hazelcast.quorum.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalMultiMap;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalMultiMapQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        TransactionContext context = getTransactionalContext(previousVersionMember);
        context.beginTransaction();
        TransactionalMultiMap<String, String> map = context.getMultiMap(name);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        count = 3;
        assertEquals(count, map.size());
        context.commitTransaction();
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
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
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalMultiMap<String, String> map = context.getMultiMap(name);
        map.put(Integer.toString(++count), "f");
        assertEquals(count, map.size());
        context.commitTransaction();
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addMultiMapConfig(
                new MultiMapConfig(name).setQuorumName("pq")
        );
    }
}
