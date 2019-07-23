package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.persistence.PersistenceTestSupport;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FencedLockPersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        HazelcastInstance apInstance = factory.newHazelcastInstance(config);

        CPSubsystem cpSubsystem = apInstance.getCPSubsystem();
        FencedLock lock1 = cpSubsystem.getLock("test");
        long fence1 = lock1.lockAndGetFence();

        FencedLock lock2 = cpSubsystem.getLock("test@group");
        long fence2 = lock2.lockAndGetFence();

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        restartInstancesParallel(addresses, config);

        assertTrue(lock1.isLockedByCurrentThread());
        assertTrue(lock2.isLockedByCurrentThread());
        assertEquals(fence1, lock1.getFence());
        assertEquals(fence2, lock2.getFence());

        lock1.unlock();
        lock2.unlock();
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        int commitIndexAdvanceCountToSnapshot = 99;
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        HazelcastInstance apInstance = factory.newHazelcastInstance(config);

        CPSubsystem cpSubsystem = apInstance.getCPSubsystem();
        FencedLock lock = cpSubsystem.getLock("test");

        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            lock.lock();
        }
        long fence = lock.getFence();

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        restartInstancesParallel(addresses, config);

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(fence, lock.getFence());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        final HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final FencedLock lock = instances[2].getCPSubsystem().getLock("test");
        long fence0 = lock.lockAndGetFence();

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        final Future<Long> f = spawn(new Callable<Long>() {
            @Override
            public Long call() {
                return lock.lockAndGetFence();
            }
        });

        // restart majority back
        instances[0] = factory.newHazelcastInstance(addresses[0], config);
        instances[1] = factory.newHazelcastInstance(addresses[1], config);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse(f.isDone());
            }
        }, 3);

        lock.unlock();

        long fence = f.get();
        assertThat(fence, greaterThan(fence0));
        assertTrue(lock.isLocked());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final FencedLock lock = instances[2].getCPSubsystem().getLock("test");
        final int count = 5000;
        final Future<Long> f = spawn(new Callable<Long>() {
            @Override
            public Long call() {
                for (int i = 0; i < count; i++) {
                    lock.lock();
                    sleepMillis(1);
                }
                return lock.getFence();
            }
        });

        sleepSeconds(1);
        // crash majority
        instances[0].getLifecycleService().terminate();

        sleepSeconds(1);
        instances[1].getLifecycleService().terminate();

        // restart majority back
        instances[1] = factory.newHazelcastInstance(addresses[1], config);
        sleepSeconds(1);
        instances[0] = factory.newHazelcastInstance(addresses[0], config);

        long fence = f.get();
        assertThat(fence, greaterThan(0L));
        assertTrue(lock.isLocked());
    }

    @Test
    public void when_CpSubsystemReset_then_dataIsRemoved() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        FencedLock lock = cpSubsystem.getLock("test");
        lock.lock();
        FencedLock lock2 = cpSubsystem.getLock("test@group");
        lock2.lock();

        cpSubsystem.getCPSubsystemManagementService().restart().get();
        long seed = getMetadataGroupId(instances[0]).seed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        lock = cpSubsystem.getLock("test");
        assertFalse(lock.isLocked());
        lock2 = cpSubsystem.getLock("test@group");
        assertFalse(lock2.isLocked());

        assertEquals(seed, getMetadataGroupId(instances[0]).seed());
    }
}
