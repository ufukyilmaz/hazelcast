package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicReference;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class RaftAtomicReferencePersistenceTest extends RaftDataStructurePersistenceTest {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        IAtomicReference<String> atomicRef = cpSubsystem.getAtomicReference("test");
        String value1 = randomString();
        atomicRef.set(value1);

        IAtomicReference<String> atomicRef2 = cpSubsystem.getAtomicReference("test@group1");
        String value2 = randomString();
        atomicRef2.set(value2);

        IAtomicReference<String> atomicRef3 = cpSubsystem.getAtomicReference("test@group2");
        String value3 = randomString();
        atomicRef3.set(value3);

        terminateMembers();
        instances = restartInstances(addresses, config);

        atomicRef = cpSubsystem.getAtomicReference("test");
        assertEquals(value1, atomicRef.get());

        atomicRef2 = cpSubsystem.getAtomicReference("test@group1");
        assertEquals(value2, atomicRef2.get());

        atomicRef3 = cpSubsystem.getAtomicReference("test@group2");
        assertEquals(value3, atomicRef3.get());
    }

    @Test
    public void when_wholeClusterRestarted_withSnapshot_then_dataIsRestored() {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        IAtomicReference<String> atomicRef = cpSubsystem.getAtomicReference("test");

        String value = null;
        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            value = randomString();
            atomicRef.set(value);
        }

        terminateMembers();
        instances = restartInstances(addresses, config);

        atomicRef = cpSubsystem.getAtomicReference("test");
        assertEquals(value, atomicRef.get());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        IAtomicReference<String> atomicRef = proxyInstance.getCPSubsystem().getAtomicReference("test");
        atomicRef.set(randomString());

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        String value = randomString();
        Future<String> f = spawn(() -> {
            atomicRef.set(value);
            return value;
        });

        // Invocation cannot complete without majority
        assertTrueAllTheTime(() -> assertFalse(f.isDone()), 3);

        // restart majority back
        instances[0] = restartInstance(addresses[0], config);
        instances[1] = restartInstance(addresses[1], config);

        assertEquals(value, f.get());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        IAtomicReference<String> atomicRef = proxyInstance.getCPSubsystem().getAtomicReference("test");
        int increments = 5000;
        AtomicReference<String> ref = new AtomicReference<>();
        Future<String> f = spawn(() -> {
            for (int i = 0; i < increments; i++) {
                String value = randomString();
                atomicRef.set(value);
                ref.set(value);
                sleepMillis(1);
            }
            return atomicRef.get();
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

        String value = f.get();
        assertEquals(ref.get(), value);
    }

    @Test
    public void whenClusterRestart_whileOperationsOngoing_then_recoversData() throws Exception {
        IAtomicReference<String> atomicRef = proxyInstance.getCPSubsystem().getAtomicReference("test");
        int increments = 5000;
        AtomicReference<String> ref = new AtomicReference<>();
        Future<String> f = spawn(() -> {
            for (int i = 0; i < increments; i++) {
                String value = randomString();
                atomicRef.set(value);
                ref.set(value);
                sleepMillis(1);
            }
            return atomicRef.get();
        });
        sleepSeconds(1);
        terminateMembers();
        restartInstances(addresses, config);

        String value = f.get();
        assertEquals(ref.get(), value);
    }

    @Test
    public void when_CPSubsystemReset_then_dataIsRemoved() throws Exception {
        CPSubsystem cpSubsystem = proxyInstance.getCPSubsystem();
        IAtomicReference<String> atomicRef = cpSubsystem.getAtomicReference("test");
        atomicRef.set(randomString());
        IAtomicReference<String> atomicRef2 = cpSubsystem.getAtomicReference("test@group");
        atomicRef2.set(randomString());

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        terminateMembers();
        instances = restartInstances(addresses, config);

        atomicRef = cpSubsystem.getAtomicReference("test");
        assertNull(atomicRef.get());
        atomicRef2 = cpSubsystem.getAtomicReference("test@group");
        assertNull(atomicRef2.get());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
    }
}