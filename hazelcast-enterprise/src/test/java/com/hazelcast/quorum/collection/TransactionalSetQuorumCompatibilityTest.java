package com.hazelcast.quorum.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalSet;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalSetQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        TransactionContext context = getTransactionalContext(previousVersionMember);
        context.beginTransaction();
        TransactionalSet<String> set = context.getSet(name);
        set.add("1");
        set.add("2");
        set.add("3");
        count = 3;
        context.commitTransaction();
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalSet<String> set = context.getSet(name);
        try {
            set.add("20");
        } finally {
            context.rollbackTransaction();
        }
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalSet<String> set = context.getSet(name);
        set.add(Integer.toString(++count));

        assertEquals(count, set.size());
        context.commitTransaction();
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addSetConfig(new SetConfig(name).setQuorumName("pq"));
    }
}
