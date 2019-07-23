package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftCountdownLatchPersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        ICountDownLatch latch1 = cpSubsystem.getCountDownLatch("test");
        latch1.trySetCount(RandomPicker.getInt(1, 100));
        int value1 = latch1.getCount();

        ICountDownLatch latch2 = cpSubsystem.getCountDownLatch("test@group1");
        latch2.trySetCount(RandomPicker.getInt(1, 100));
        int value2 = latch2.getCount();

        ICountDownLatch latch3 = cpSubsystem.getCountDownLatch("test@group2");
        latch3.trySetCount(RandomPicker.getInt(1, 100));
        int value3 = latch3.getCount();

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        latch1 = cpSubsystem.getCountDownLatch("test");
        assertEquals(value1, latch1.getCount());

        latch2 = cpSubsystem.getCountDownLatch("test@group1");
        assertEquals(value2, latch2.getCount());

        latch3 = cpSubsystem.getCountDownLatch("test@group2");
        assertEquals(value3, latch3.getCount());
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        int commitIndexAdvanceCountToSnapshot = 99;
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        ICountDownLatch latch = cpSubsystem.getCountDownLatch("test");
        latch.trySetCount(1000);

        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            latch.countDown();
        }
        int available = latch.getCount();

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        latch = cpSubsystem.getCountDownLatch("test");
        assertEquals(available, latch.getCount());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        final HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final ICountDownLatch latch = instances[2].getCPSubsystem().getCountDownLatch("test");
        latch.trySetCount(1);

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        final Future<Void> f = spawn(new Callable<Void>() {
            @Override
            public Void call() {
                latch.countDown();
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
        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final ICountDownLatch latch = instances[2].getCPSubsystem().getCountDownLatch("test");
        final int permits = 5000;
        latch.trySetCount(permits);
        final Future<Void> f = spawn(new Callable<Void>() {
            @Override
            public Void call() {
                for (int i = 0; i < permits; i++) {
                    latch.countDown();
                    sleepMillis(1);
                }
                return null;
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

        f.get();
        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void when_CpSubsystemReset_then_dataIsRemoved() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        ICountDownLatch latch = cpSubsystem.getCountDownLatch("test");
        latch.trySetCount(10);
        ICountDownLatch latch2 = cpSubsystem.getCountDownLatch("test@group");
        latch2.trySetCount(100);

        cpSubsystem.getCPSubsystemManagementService().restart().get();
        long seed = getMetadataGroupId(instances[0]).seed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        latch = cpSubsystem.getCountDownLatch("test");
        assertEquals(0, latch.getCount());
        latch2 = cpSubsystem.getCountDownLatch("test@group");
        assertEquals(0, latch2.getCount());

        assertEquals(seed, getMetadataGroupId(instances[0]).seed());
    }

    @Test
    public void when_CPmembersRestart_whileAPMemberBlocked() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        HazelcastInstance apInstance = factory.newHazelcastInstance(config);

        final ICountDownLatch latch = apInstance.getCPSubsystem().getCountDownLatch("test");
        latch.trySetCount(1);

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        Future<Object> f = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                latch.await(2, TimeUnit.MINUTES);
                return null;
            }
        });

        restartInstances(addresses, config);
        latch.countDown();

        f.get();
        assertEquals(0, latch.getCount());
    }
}
