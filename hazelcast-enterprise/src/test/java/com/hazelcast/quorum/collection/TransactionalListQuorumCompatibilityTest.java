package com.hazelcast.quorum.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalList;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class TransactionalListQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        TransactionContext context = getTransactionalContext(previousVersionMember);
        context.beginTransaction();
        TransactionalList<String> set = context.getList(name);
        set.add("1");
        set.add("2");
        set.add("3");
        context.commitTransaction();
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalList<String> set = context.getList(name);
        // no quorum applies while operating in 3.9 cluster version
        assertEquals(3, set.size());
        for (int i = 10; i < 20; i++) {
            set.add(Integer.toString(i));
        }
        context.commitTransaction();
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalList<String> setOnCurrentVersion = context.getList(name);
        try {
            setOnCurrentVersion.add("20");
        } finally {
            context.rollbackTransaction();
        }
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalList<String> setOnCurrentVersion = context.getList(name);
        for (int i = 20; i < 30; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }

        assertEquals(23, setOnCurrentVersion.size());
        context.commitTransaction();
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addListConfig(new ListConfig(name).setQuorumName("pq"));
    }
}
