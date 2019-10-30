package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RaftAtomicLongPersistenceTest extends RaftDataStructurePersistenceTest {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        IAtomicLong atomicLong = cpSubsystem.getAtomicLong("test");
        long value1 = atomicLong.incrementAndGet();

        IAtomicLong atomicLong2 = cpSubsystem.getAtomicLong("test@group1");
        long value2 = atomicLong2.addAndGet(RandomPicker.getInt(100));

        IAtomicLong atomicLong3 = cpSubsystem.getAtomicLong("test@group2");
        long value3 = atomicLong3.decrementAndGet();

        terminateMembers();
        instances = restartInstances(addresses, config);

        atomicLong = cpSubsystem.getAtomicLong("test");
        assertEquals(value1, atomicLong.get());

        atomicLong2 = cpSubsystem.getAtomicLong("test@group1");
        assertEquals(value2, atomicLong2.get());

        atomicLong3 = cpSubsystem.getAtomicLong("test@group2");
        assertEquals(value3, atomicLong3.get());
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        IAtomicLong atomicLong = cpSubsystem.getAtomicLong("test");

        long value = 0;
        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            value = atomicLong.incrementAndGet();
        }

        terminateMembers();

        instances = restartInstances(addresses, config);

        atomicLong = cpSubsystem.getAtomicLong("test");
        assertEquals(value, atomicLong.get());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        IAtomicLong atomicLong = proxyInstance.getCPSubsystem().getAtomicLong("test");
        long value = atomicLong.addAndGet(RandomPicker.getInt(100));

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        Future<Long> f = spawn(atomicLong::incrementAndGet);

        // Invocation cannot complete without majority
        assertTrueAllTheTime(() -> assertFalse(f.isDone()), 3);

        // restart majority back
        instances[0] = restartInstance(addresses[0], config);
        instances[1] = restartInstance(addresses[1], config);

        Long newValue = f.get();
        assertEquals(value, newValue - 1);
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        IAtomicLong atomicLong = proxyInstance.getCPSubsystem().getAtomicLong("test");
        AtomicLong increments = new AtomicLong();
        AtomicBoolean done = new AtomicBoolean();
        Future<Long> f = spawn(() -> {
            while (!done.get()) {
                atomicLong.incrementAndGet();
                increments.incrementAndGet();
                LockSupport.parkNanos(1);
            }
            return atomicLong.get();
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

        sleepSeconds(1);
        done.set(true);

        long value = f.get();
        long expected = increments.get();
        assertBetween("atomiclong value", value, expected, expected + 1);
    }

    @Test
    public void whenClusterRestart_whileOperationsOngoing_then_recoversData() throws Exception {
        IAtomicLong atomicLong = proxyInstance.getCPSubsystem().getAtomicLong("test");
        AtomicLong increments = new AtomicLong();
        AtomicBoolean done = new AtomicBoolean();
        Future<Long> f = spawn(() -> {
            while (!done.get()) {
                atomicLong.incrementAndGet();
                increments.incrementAndGet();
                LockSupport.parkNanos(1);
            }
            return atomicLong.get();
        });

        sleepSeconds(1);
        terminateMembers();
        restartInstances(addresses, config);

        sleepSeconds(1);
        done.set(true);

        long value = f.get();
        long expected = increments.get();
        assertBetween("atomiclong value", value, expected, expected + 1);
    }

    @Test
    public void when_CPSubsystemReset_then_dataIsRemoved() throws Exception {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        IAtomicLong atomicLong = cpSubsystem.getAtomicLong("test");
        atomicLong.addAndGet(RandomPicker.getInt(100));
        IAtomicLong atomicLong2 = cpSubsystem.getAtomicLong("test@group");
        atomicLong2.addAndGet(RandomPicker.getInt(100));

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        terminateMembers();
        instances = restartInstances(addresses, config);

        atomicLong = cpSubsystem.getAtomicLong("test");
        assertEquals(0, atomicLong.get());
        atomicLong = cpSubsystem.getAtomicLong("test@group");
        assertEquals(0, atomicLong.get());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
    }
}
