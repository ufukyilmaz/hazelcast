package com.hazelcast.splitbrainprotection.executor;

import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.splitbrainprotection.executor.CompatibilityTestCallable.RESPONSE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class DurableExecutorSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        submitCallable(previousVersionMember);
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionAbsent(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected void assertOperations_whileSplitBrainProtectionPresent(HazelcastInstance member) {
        submitCallable(member);
    }

    @Override
    protected Config getSplitBrainProtectedConfig() {
        return getConfigWithSplitBrainProtection()
                .addDurableExecutorConfig(new DurableExecutorConfig(name).setSplitBrainProtectionName("pq"));
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
