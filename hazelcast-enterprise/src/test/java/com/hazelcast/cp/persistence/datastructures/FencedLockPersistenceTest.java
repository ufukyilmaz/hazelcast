package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.Future;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockPersistenceTest extends RaftDataStructurePersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        FencedLock lock1 = cpSubsystem.getLock("test");
        long fence1 = lock1.lockAndGetFence();

        FencedLock lock2 = cpSubsystem.getLock("test@group");
        long fence2 = lock2.lockAndGetFence();

        terminateMembers();
        restartInstances(addresses, config);

        assertTrue(lock1.isLockedByCurrentThread());
        assertTrue(lock2.isLockedByCurrentThread());
        assertEquals(fence1, lock1.getFence());
        assertEquals(fence2, lock2.getFence());

        lock1.unlock();
        lock2.unlock();
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        FencedLock lock = cpSubsystem.getLock("test");

        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            lock.lock();
        }
        long fence = lock.getFence();

        terminateMembers();
        restartInstances(addresses, config);

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(fence, lock.getFence());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        FencedLock lock = proxyInstance.getCPSubsystem().getLock("test");
        long fence0 = lock.lockAndGetFence();

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        Future<Long> f = spawn(lock::lockAndGetFence);

        // restart majority back
        instances[0] = restartInstance(addresses[0], config);
        instances[1] = restartInstance(addresses[1], config);

        assertTrueAllTheTime(() -> assertFalse(f.isDone()), 3);

        lock.unlock();

        long fence = f.get();
        assertThat(fence, greaterThan(fence0));
        assertTrue(lock.isLocked());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        FencedLock lock = proxyInstance.getCPSubsystem().getLock("test");
        int count = 5000;
        Future<Long> f = spawn(() -> {
            for (int i = 0; i < count; i++) {
                lock.lock();
                sleepMillis(1);
            }
            return lock.getFence();
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

        long fence = f.get();
        assertThat(fence, greaterThan(0L));
        assertTrue(lock.isLocked());
    }

    @Test
    public void when_CPSubsystemReset_then_dataIsRemoved() throws Exception {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        FencedLock lock = cpSubsystem.getLock("test");
        lock.lock();
        FencedLock lock2 = cpSubsystem.getLock("test@group");
        lock2.lock();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().restart().toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        terminateMembers();
        instances = restartInstances(addresses, config);

        lock = cpSubsystem.getLock("test");
        assertFalse(lock.isLocked());
        lock2 = cpSubsystem.getLock("test@group");
        assertFalse(lock2.isLocked());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
    }

    protected HazelcastInstance createProxyInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }
}
