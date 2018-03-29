package com.hazelcast.quorum.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.quorum.QuorumCompatibilityTest;
import com.hazelcast.transaction.TransactionContext;

import static org.junit.Assert.assertEquals;

public class TransactionalSetQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        TransactionContext context = getTransactionalContext(previousVersionMember);
        context.beginTransaction();
        TransactionalSet<String> set = context.getSet(name);
        set.add("1");
        set.add("2");
        set.add("3");
        context.commitTransaction();
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        TransactionContext context = getTransactionalContext(member);
        context.beginTransaction();
        TransactionalSet<String> set = context.getSet(name);
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
        TransactionalSet<String> setOnCurrentVersion = context.getSet(name);
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
        TransactionalSet<String> setOnCurrentVersion = context.getSet(name);
        for (int i = 20; i < 30; i++) {
            setOnCurrentVersion.add(Integer.toString(i));
        }

        assertEquals(23, setOnCurrentVersion.size());
        context.commitTransaction();
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addSetConfig(new SetConfig(name).setQuorumName("pq"));
    }
}
