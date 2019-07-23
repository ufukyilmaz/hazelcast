package com.hazelcast.cp.persistence.datastructures;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.cp.CPSubsystem;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicReferencePersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_dataIsRestored() {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        IAtomicReference<String> atomicRef = cpSubsystem.getAtomicReference("test");
        String value1 = randomString();
        atomicRef.set(value1);

        IAtomicReference<String> atomicRef2 = cpSubsystem.getAtomicReference("test@group1");
        String value2 = randomString();
        atomicRef2.set(value2);

        IAtomicReference<String> atomicRef3 = cpSubsystem.getAtomicReference("test@group2");
        String value3 = randomString();
        atomicRef3.set(value3);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        atomicRef = cpSubsystem.getAtomicReference("test");
        assertEquals(value1, atomicRef.get());

        atomicRef2 = cpSubsystem.getAtomicReference("test@group1");
        assertEquals(value2, atomicRef2.get());

        atomicRef3 = cpSubsystem.getAtomicReference("test@group2");
        assertEquals(value3, atomicRef3.get());
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
        IAtomicReference<String> atomicRef = cpSubsystem.getAtomicReference("test");

        String value = null;
        for (int j = 0; j < commitIndexAdvanceCountToSnapshot + 10; j++) {
            value = randomString();
            atomicRef.set(value);
        }

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        atomicRef = cpSubsystem.getAtomicReference("test");
        assertEquals(value, atomicRef.get());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        final HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final IAtomicReference<String> atomicRef = instances[2].getCPSubsystem().getAtomicReference("test");
        atomicRef.set(randomString());

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        final String value = randomString();
        final Future<String> f = spawn(new Callable<String>() {
            @Override
            public String call() {
                atomicRef.set(value);
                return value;
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

        assertEquals(value, f.get());
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final IAtomicReference<String> atomicRef = instances[2].getCPSubsystem().getAtomicReference("test");
        final int increments = 5000;
        final AtomicReference<String> ref = new AtomicReference<String>();
        final Future<String> f = spawn(new Callable<String>() {
            @Override
            public String call() {
                for (int i = 0; i < increments; i++) {
                    String value = randomString();
                    atomicRef.set(value);
                    ref.set(value);
                    sleepMillis(1);
                }
                return atomicRef.get();
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

        String value = f.get();
        assertEquals(ref.get(), value);
    }

    @Test
    public void when_CpSubsystemReset_then_dataIsRemoved() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        CPSubsystem cpSubsystem = instances[0].getCPSubsystem();
        IAtomicReference<String> atomicRef = cpSubsystem.getAtomicReference("test");
        atomicRef.set(randomString());
        IAtomicReference<String> atomicRef2 = cpSubsystem.getAtomicReference("test@group");
        atomicRef2.set(randomString());

        cpSubsystem.getCPSubsystemManagementService().restart().get();
        long seed = getMetadataGroupId(instances[0]).seed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        cpSubsystem = instances[0].getCPSubsystem();

        atomicRef = cpSubsystem.getAtomicReference("test");
        assertNull(atomicRef.get());
        atomicRef2 = cpSubsystem.getAtomicReference("test@group");
        assertNull(atomicRef2.get());

        assertEquals(seed, getMetadataGroupId(instances[0]).seed());
    }
}
