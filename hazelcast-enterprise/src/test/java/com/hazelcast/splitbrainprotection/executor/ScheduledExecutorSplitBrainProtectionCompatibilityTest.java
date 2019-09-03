package com.hazelcast.splitbrainprotection.executor;

import com.hazelcast.config.Config;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionCompatibilityTest;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.splitbrainprotection.executor.CompatibilityTestCallable.RESPONSE;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ScheduledExecutorSplitBrainProtectionCompatibilityTest extends AbstractSplitBrainProtectionCompatibilityTest {

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
                .addScheduledExecutorConfig(new ScheduledExecutorConfig(name).setSplitBrainProtectionName("pq"));
    }

    private void submitCallable(HazelcastInstance member) {
        IScheduledExecutorService executorService = member.getScheduledExecutorService(name);
        IScheduledFuture<String> future = executorService.schedule(new CompatibilityTestCallable(), 1, TimeUnit.MILLISECONDS);
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
