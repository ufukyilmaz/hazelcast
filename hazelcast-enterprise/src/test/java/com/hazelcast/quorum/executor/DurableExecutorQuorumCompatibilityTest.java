package com.hazelcast.quorum.executor;

import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.quorum.QuorumCompatibilityTest;

import java.util.concurrent.Future;

import static com.hazelcast.quorum.executor.CompatibilityTestCallable.RESPONSE;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;

public class DurableExecutorQuorumCompatibilityTest extends QuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        submitCallable(previousVersionMember);
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addDurableExecutorConfig(new DurableExecutorConfig(name).setQuorumName("pq"));
    }

    private void submitCallable(HazelcastInstance member) {
        DurableExecutorService executorService = member.getDurableExecutorService(name);
        Future<String> future = executorService.submit(new CompatibilityTestCallable());
        assertEquals(RESPONSE, getResponse(future));
    }

    private String getResponse(Future<String> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
