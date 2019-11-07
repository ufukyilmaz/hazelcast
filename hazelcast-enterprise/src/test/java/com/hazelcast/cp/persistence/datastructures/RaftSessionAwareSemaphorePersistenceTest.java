package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.cp.ISemaphore;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class RaftSessionAwareSemaphorePersistenceTest extends AbstractRaftSemaphorePersistenceTest {

    @Override
    protected Config createConfig(int cpMemberCount, int groupSize) {
        Config config = super.createConfig(cpMemberCount, groupSize);
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig("*", false, 0);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return config;
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        ISemaphore semaphore = proxyInstance.getCPSubsystem().getSemaphore("test");
        int acquires = 5000;
        semaphore.init(acquires);
        Future<Integer> f = spawn(() -> {
            for (int i = 0; i < acquires; i++) {
                semaphore.acquire();
                sleepMillis(1);
            }
            return semaphore.availablePermits();
        });

        sleepSeconds(1);
        // crash majority
        instances[0].getLifecycleService().terminate();

        sleepSeconds(1);
        instances[1].getLifecycleService().terminate();

        // restart majority back
        instances[1] = restartInstance(addresses[1], config);
        sleepSeconds(1);
        instances[0] = restartInstance(addresses[0], config);

        int value = f.get();
        assertEquals("Remaining permits", 0, value);
    }
}
