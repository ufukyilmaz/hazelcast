package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
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
public class RaftAtomicLongPersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        IAtomicLong atomicLong = cpSubsystem.getAtomicLong("test");
        long value1 = atomicLong.incrementAndGet();

        IAtomicLong atomicLong2 = cpSubsystem.getAtomicLong("test@group1");
        long value2 = atomicLong2.addAndGet(RandomPicker.getInt(100));

        IAtomicLong atomicLong3 = cpSubsystem.getAtomicLong("test@group2");
        long value3 = atomicLong3.decrementAndGet();

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        atomicLong = cpSubsystem.getAtomicLong("test");
        assertEquals(value1, atomicLong.get());

        atomicLong2 = cpSubsystem.getAtomicLong("test@group1");
        assertEquals(value2, atomicLong2.get());

        atomicLong3 = cpSubsystem.getAtomicLong("test@group2");
        assertEquals(value3, atomicLong3.get());
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
        IAtomicLong atomicLong = cpSubsystem.getAtomicLong("test");

        long value = 0;
        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            value = atomicLong.incrementAndGet();
        }

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        atomicLong = cpSubsystem.getAtomicLong("test");
        assertEquals(value, atomicLong.get());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        final HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final IAtomicLong atomicLong = instances[2].getCPSubsystem().getAtomicLong("test");
        long value = atomicLong.addAndGet(RandomPicker.getInt(100));

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        final Future<Long> f = spawn(new Callable<Long>() {
            @Override
            public Long call() {
                return atomicLong.incrementAndGet();
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

        Long newValue = f.get();
        assertEquals(value, newValue - 1);
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final IAtomicLong atomicLong = instances[2].getCPSubsystem().getAtomicLong("test");
        final int increments = 5000;
        final Future<Long> f = spawn(new Callable<Long>() {
            @Override
            public Long call() {
                for (int i = 0; i < increments; i++) {
                    atomicLong.incrementAndGet();
                    sleepMillis(1);
                }
                return atomicLong.get();
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

        long value = f.get();
        assertBetween("atomiclong value", value, increments, increments + 1);
    }

    @Test
    public void when_CpSubsystemReset_then_dataIsRemoved() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        IAtomicLong atomicLong = cpSubsystem.getAtomicLong("test");
        atomicLong.addAndGet(RandomPicker.getInt(100));
        IAtomicLong atomicLong2 = cpSubsystem.getAtomicLong("test@group");
        atomicLong2.addAndGet(RandomPicker.getInt(100));

        cpSubsystem.getCPSubsystemManagementService().restart().get();
        long seed = getMetadataGroupId(instances[0]).seed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        atomicLong = cpSubsystem.getAtomicLong("test");
        assertEquals(0, atomicLong.get());
        atomicLong = cpSubsystem.getAtomicLong("test@group");
        assertEquals(0, atomicLong.get());

        assertEquals(seed, getMetadataGroupId(instances[0]).seed());
    }
}
