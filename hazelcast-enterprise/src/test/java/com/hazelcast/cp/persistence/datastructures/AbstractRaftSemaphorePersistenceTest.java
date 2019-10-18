package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public abstract class AbstractRaftSemaphorePersistenceTest extends RaftDataStructurePersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        ISemaphore semaphore1 = cpSubsystem.getSemaphore("test");
        semaphore1.init(RandomPicker.getInt(1, 100));
        int value1 = semaphore1.availablePermits();

        ISemaphore semaphore2 = cpSubsystem.getSemaphore("test@group1");
        semaphore2.init(RandomPicker.getInt(1, 100));
        int value2 = semaphore2.availablePermits();

        ISemaphore semaphore3 = cpSubsystem.getSemaphore("test@group2");
        semaphore3.init(RandomPicker.getInt(1, 100));
        int value3 = semaphore3.availablePermits();

        terminateMembers();
        instances = restartInstances(addresses, config);

        semaphore1 = cpSubsystem.getSemaphore("test");
        assertEquals(value1, semaphore1.availablePermits());

        semaphore2 = cpSubsystem.getSemaphore("test@group1");
        assertEquals(value2, semaphore2.availablePermits());

        semaphore3 = cpSubsystem.getSemaphore("test@group2");
        assertEquals(value3, semaphore3.availablePermits());
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() throws Exception {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        ISemaphore semaphore = cpSubsystem.getSemaphore("test");
        semaphore.init(1000);

        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            semaphore.acquire();
        }
        int availablePermits = semaphore.availablePermits();

        terminateMembers();
        instances = restartInstances(addresses, config);

        semaphore = cpSubsystem.getSemaphore("test");
        assertEquals(availablePermits, semaphore.availablePermits());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        ISemaphore semaphore = proxyInstance.getCPSubsystem().getSemaphore("test");
        semaphore.init(10);

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        Future<Void> f = spawn(() -> {
            semaphore.acquire();
            return null;
        });

        // Invocation cannot complete without majority
        assertTrueAllTheTime(() -> assertFalse(f.isDone()), 3);

        // restart majority back
        instances[0] = restartInstance(addresses[0], config);
        instances[1] = restartInstance(addresses[1], config);

        f.get();
        assertEquals(9, semaphore.availablePermits());
    }

    @Test
    @Ignore
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        ISemaphore semaphore = proxyInstance.getCPSubsystem().getSemaphore("test");
        int acquires = 5000;
        semaphore.init(acquires + 1); // +1 permit for indeterminate retry
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
        assertBetween("Remaining permits", value, 0, 1);
    }

    @Test
    public void when_cpSubsystemReset_then_dataIsRemoved() throws Exception {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        ISemaphore semaphore = cpSubsystem.getSemaphore("test");
        semaphore.init(10);
        ISemaphore semaphore2 = cpSubsystem.getSemaphore("test@group");
        semaphore2.init(100);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset()
                    .toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        terminateMembers();
        instances = restartInstances(addresses, config);

        semaphore = cpSubsystem.getSemaphore("test");
        assertEquals(0, semaphore.availablePermits());
        semaphore2 = cpSubsystem.getSemaphore("test@group");
        assertEquals(0, semaphore2.availablePermits());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
    }

    @Test
    public void when_cpMembersRestart_whileInvocationBlocked() throws Exception {
        ISemaphore semaphore = proxyInstance.getCPSubsystem().getSemaphore("test");
        semaphore.init(1);

        terminateMembers();

        Future<Object> f = spawn(() -> {
            semaphore.acquire();
            return null;
        });

        restartInstances(addresses, config);

        f.get();
        assertEquals(0, semaphore.availablePermits());
    }
}
