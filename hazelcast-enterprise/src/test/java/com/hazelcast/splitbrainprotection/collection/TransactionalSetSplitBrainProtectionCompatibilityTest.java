package com.hazelcast.splitbrainprotection.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalSet;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalSetSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {

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
    protected void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member) {
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
    protected void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalSet<String> set = context.getSet(name);
        set.add(Integer.toString(++count));

        assertEquals(count, set.size());
        context.commitTransaction();
    }

    @Override
    protected Config getSplitBrainProtectedConfig() {
        return getConfigWithSplitBrainProtection().addSetConfig(new SetConfig(name).setSplitBrainProtectionName("pq"));
    }
}
