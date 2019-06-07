package com.hazelcast.quorum.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalList;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalListQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    private int count;

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        TransactionContext context = getTransactionalContext(previousVersionMember);
        context.beginTransaction();
        TransactionalList<String> list = context.getList(name);
        list.add("1");
        list.add("2");
        list.add("3");
        context.commitTransaction();
        count = 3;
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalList<String> listOnCurrentVersion = context.getList(name);
        try {
            listOnCurrentVersion.add("20");
        } finally {
            context.rollbackTransaction();
        }
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalList<String> listOnCurrentVersion = context.getList(name);
        for (int i = 20; i < 30; i++) {
            listOnCurrentVersion.add(Integer.toString(i));
            count++;
        }

        assertEquals(count, listOnCurrentVersion.size());
        context.commitTransaction();
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addListConfig(new ListConfig(name).setQuorumName("pq"));
    }
}
