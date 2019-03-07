package com.hazelcast.quorum.executor;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.quorum.executor.CompatibilityTestCallable.RESPONSE;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({CompatibilityTest.class})
public class ExecutorQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        submitCallable(previousVersionMember);
    }

    @Override
    protected void assertOperations_whileQuorumAbsent(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected void assertOperations_whileQuorumPresent(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum()
                .addExecutorConfig(new ExecutorConfig(name).setQuorumName("pq"));
    }

    private void submitCallable(HazelcastInstance member) {
        IExecutorService executorService = member.getExecutorService(name);
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