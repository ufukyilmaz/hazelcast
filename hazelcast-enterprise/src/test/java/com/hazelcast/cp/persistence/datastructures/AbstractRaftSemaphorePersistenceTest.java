package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.persistence.PersistenceTestSupport;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public abstract class AbstractRaftSemaphorePersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        ISemaphore semaphore1 = cpSubsystem.getSemaphore("test");
        semaphore1.init(RandomPicker.getInt(1, 100));
        int value1 = semaphore1.availablePermits();

        ISemaphore semaphore2 = cpSubsystem.getSemaphore("test@group1");
        semaphore2.init(RandomPicker.getInt(1, 100));
        int value2 = semaphore2.availablePermits();

        ISemaphore semaphore3 = cpSubsystem.getSemaphore("test@group2");
        semaphore3.init(RandomPicker.getInt(1, 100));
        int value3 = semaphore3.availablePermits();

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        semaphore1 = cpSubsystem.getSemaphore("test");
        assertEquals(value1, semaphore1.availablePermits());

        semaphore2 = cpSubsystem.getSemaphore("test@group1");
        assertEquals(value2, semaphore2.availablePermits());

        semaphore3 = cpSubsystem.getSemaphore("test@group2");
        assertEquals(value3, semaphore3.availablePermits());
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() throws Exception {
        Config config = createConfig(3, 3);
        int commitIndexAdvanceCountToSnapshot = 99;
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        ISemaphore semaphore = cpSubsystem.getSemaphore("test");
        semaphore.init(1000);

        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            semaphore.acquire();
        }
        int availablePermits = semaphore.availablePermits();

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        semaphore = cpSubsystem.getSemaphore("test");
        assertEquals(availablePermits, semaphore.availablePermits());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        final HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final ISemaphore semaphore = instances[2].getCPSubsystem().getSemaphore("test");
        semaphore.init(10);

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        final Future<Void> f = spawn(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                semaphore.acquire();
                return null;
            }
        });

        // Invocation cannot complete without majority
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse(f.isDone());
            }
        }, 3);

        // restart majority back
        instances[0] = factory.newHazelcastInstance(addresses[0], config);
        instances[1] = factory.newHazelcastInstance(addresses[1], config);

        f.get();
        assertEquals(9, semaphore.availablePermits());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final ISemaphore semaphore = instances[2].getCPSubsystem().getSemaphore("test");
        final int acquires = 5000;
        semaphore.init(acquires + 1); // +1 permit for indeterminate retry
        final Future<Integer> f = spawn(new Callable<Integer>() {
            @Override
            public Integer call() throws InterruptedException {
                for (int i = 0; i < acquires; i++) {
                    semaphore.acquire();
                    sleepMillis(1);
                }
                return semaphore.availablePermits();
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

        int value = f.get();
        assertBetween("Remaining permits", value, 0, 1);
    }

    @Test
    public void when_CpSubsystemReset_then_dataIsRemoved() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        ISemaphore semaphore = cpSubsystem.getSemaphore("test");
        semaphore.init(10);
        ISemaphore semaphore2 = cpSubsystem.getSemaphore("test@group");
        semaphore2.init(100);

        cpSubsystem.getCPSubsystemManagementService().restart().get();
        long seed = getMetadataGroupId(instances[0]).seed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        semaphore = cpSubsystem.getSemaphore("test");
        assertEquals(0, semaphore.availablePermits());
        semaphore2 = cpSubsystem.getSemaphore("test@group");
        assertEquals(0, semaphore2.availablePermits());

        assertEquals(seed, getMetadataGroupId(instances[0]).seed());
    }

    @Test
    public void when_CPmembersRestart_whileAPMemberBlocked() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        HazelcastInstance apInstance = factory.newHazelcastInstance(config);

        final ISemaphore semaphore = apInstance.getCPSubsystem().getSemaphore("test");
        semaphore.init(1);

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        Future<Object> f = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                semaphore.acquire();
                return null;
            }
        });

        instances = restartInstances(addresses, config);

        f.get();
        assertEquals(0, semaphore.availablePermits());
    }
}
