package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftCountdownLatchPersistenceTest extends RaftDataStructurePersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        ICountDownLatch latch1 = cpSubsystem.getCountDownLatch("test");
        latch1.trySetCount(RandomPicker.getInt(1, 100));
        int value1 = latch1.getCount();

        ICountDownLatch latch2 = cpSubsystem.getCountDownLatch("test@group1");
        latch2.trySetCount(RandomPicker.getInt(1, 100));
        int value2 = latch2.getCount();

        ICountDownLatch latch3 = cpSubsystem.getCountDownLatch("test@group2");
        latch3.trySetCount(RandomPicker.getInt(1, 100));
        int value3 = latch3.getCount();

        terminateMembers();
        instances = restartInstances(addresses, config);

        latch1 = cpSubsystem.getCountDownLatch("test");
        assertEquals(value1, latch1.getCount());

        latch2 = cpSubsystem.getCountDownLatch("test@group1");
        assertEquals(value2, latch2.getCount());

        latch3 = cpSubsystem.getCountDownLatch("test@group2");
        assertEquals(value3, latch3.getCount());
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        ICountDownLatch latch = cpSubsystem.getCountDownLatch("test");
        latch.trySetCount(1000);

        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            latch.countDown();
        }
        int available = latch.getCount();

        terminateMembers();
        instances = restartInstances(addresses, config);

        latch = cpSubsystem.getCountDownLatch("test");
        assertEquals(available, latch.getCount());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        ICountDownLatch latch = proxyInstance.getCPSubsystem().getCountDownLatch("test");
        latch.trySetCount(1);

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        Future<Void> f = spawn(() -> {

            latch.countDown();
            return null;
        });

        // Invocation cannot complete without majority
        assertTrueAllTheTime(() -> assertFalse(f.isDone()), 3);

        // restart majority back
        instances[0] = restartInstance(addresses[0], config);
        instances[1] = restartInstance(addresses[1], config);

        f.get();
        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        ICountDownLatch latch = proxyInstance.getCPSubsystem().getCountDownLatch("test");
        int permits = 5000;
        latch.trySetCount(permits);
        Future<Void> f = spawn(() -> {
            for (int i = 0; i < permits; i++) {
                latch.countDown();
                sleepMillis(1);
            }
            return null;
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

        f.get();
        assertOpenEventually(latch);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void when_CPSubsystemReset_then_dataIsRemoved() throws Exception {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        ICountDownLatch latch = cpSubsystem.getCountDownLatch("test");
        latch.trySetCount(10);
        ICountDownLatch latch2 = cpSubsystem.getCountDownLatch("test@group");
        latch2.trySetCount(100);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().restart()
                    .toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        terminateMembers();
        instances = restartInstances(addresses, config);

        latch = cpSubsystem.getCountDownLatch("test");
        assertEquals(0, latch.getCount());
        latch2 = cpSubsystem.getCountDownLatch("test@group");
        assertEquals(0, latch2.getCount());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
    }

    @Test
    public void when_CPMembersRestart_whileAPMemberBlocked() throws Exception {
        ICountDownLatch latch = proxyInstance.getCPSubsystem().getCountDownLatch("test");
        latch.trySetCount(1);

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        Future<Object> f = spawn(() -> {
            latch.await(2, TimeUnit.MINUTES);
            return null;
        });

        restartInstances(addresses, config);
        latch.countDown();

        f.get();
        assertEquals(0, latch.getCount());
    }
}
