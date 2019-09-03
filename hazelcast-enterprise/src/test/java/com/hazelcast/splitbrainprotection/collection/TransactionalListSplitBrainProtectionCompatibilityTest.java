package com.hazelcast.splitbrainprotection.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalList;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalListSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {

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
    protected void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member) {
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
    protected void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member) {
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
    protected Config getSplitBrainProtectedConfig() {
        return getConfigWithSplitBrainProtection().addListConfig(new ListConfig(name).setSplitBrainProtectionName("pq"));
    }
}
