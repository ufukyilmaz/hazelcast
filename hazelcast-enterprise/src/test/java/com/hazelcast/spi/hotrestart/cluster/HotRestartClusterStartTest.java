package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.IndeterminateOperationStateExceptionTest.PrimaryOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.AntiEntropyCorrectnessTest.setBackupPacketDropFilter;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterStartTest extends AbstractHotRestartClusterStartTest {

    @Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{NONE, PARTIAL, ALL});
    }

    @Test
    public void testSingleMemberRestart() {
        Address address = startAndTerminateInstance();
        HazelcastInstance hz = restartInstance(address);
        assertInstancesJoined(1, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(hz);
    }

    @Test
    public void test_hotRestart_whenClusterState_ACTIVE() {
        Address[] addresses = startAndTerminateInstances(4);

        HazelcastInstance[] instances = restartInstances(addresses);
        assertInstancesJoined(4, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_hotRestart_withMigration() {
        HazelcastInstance[] instances1 = startNewInstances(2);
        warmUpPartitions(instances1);

        HazelcastInstance[] instances2 = startNewInstances(2);

        HazelcastInstance[] instances = new HazelcastInstance[4];
        System.arraycopy(instances1, 0, instances, 0, 2);
        System.arraycopy(instances2, 0, instances, 2, 2);

        assertInstancesJoined(4, NodeState.ACTIVE, ClusterState.ACTIVE);
        waitAllForSafeState(instances);

        Address[] addresses = getAddresses(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);
        terminateInstances();

        instances = restartInstances(addresses);
        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.FROZEN);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_hotRestart_whenClusterState_FROZEN() {
        HazelcastInstance[] instances = startNewInstances(4);

        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        instances = restartInstances(addresses);
        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.FROZEN);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_hotRestart_whenClusterState_PASSIVE() {
        HazelcastInstance[] instances = startNewInstances(4);

        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        instances = restartInstances(addresses);
        assertInstancesJoined(4, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_multipleRestartsWithSingleNode_whenClusterState_PASSIVE() {
        testMultipleRestartsWhenClusterPASSIVE(1);
    }

    @Test
    public void test_multipleRestartsWithMultipleNodes_whenClusterState_PASSIVE() {
        testMultipleRestartsWhenClusterPASSIVE(4);
    }

    private void testMultipleRestartsWhenClusterPASSIVE(int nodeCount) {
        HazelcastInstance[] instances = startNewInstances(nodeCount);

        assertInstancesJoined(nodeCount, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        instances = restartInstances(addresses);
        assertInstancesJoined(nodeCount, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
        invokeDummyOperationOnAllPartitions(instances);

        addresses = getAddresses(instances);
        terminateInstances();

        instances = restartInstances(addresses);
        assertInstancesJoined(nodeCount, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_hotRestartFails_withMissingHotRestartDirectory_forOneNode() {
        Address[] addresses = startAndTerminateInstances(4);

        Address randomAddress = addresses[RandomPicker.getInt(addresses.length)];
        deleteHotRestartDirectoryOfNode(randomAddress);

        HazelcastInstance[] instances = restartInstances(addresses);
        if (instances.length == 1) {
            HazelcastInstance instance = instances[0];
            assertTrue(getClusterService(instance).isJoined());
            assertClusterSizeEventually(1, instance);
        } else {
            assertEquals(0, instances.length);
        }
    }

    @Test
    public void test_hotRestartFails_whenNodeStartsBeforeOthers_withMissingHotRestartDirectory()  {
        Address[] addresses = startAndTerminateInstances(4);

        Address randomAddress = addresses[RandomPicker.getInt(addresses.length)];
        deleteHotRestartDirectoryOfNode(randomAddress);

        HazelcastInstance instance = restartInstance(randomAddress);
        assertTrue(getClusterService(instance).isJoined());

        HazelcastInstance[] instances = restartInstances(removeAddress(addresses, randomAddress));
        assertEquals(0, instances.length);

        assertClusterSizeEventually(1, instance);
    }

    private static Address[] removeAddress(Address[] addresses, Address address) {
        Address[] newAddresses = new Address[addresses.length - 1];
        int ix = 0;
        for (Address a : addresses) {
            if (a.equals(address)) {
                continue;
            }
            newAddresses[ix++] = a;
        }
        return newAddresses;
    }

    @Test
    public void test_hotRestartFails_withUnknownNode() {
        Address[] addresses = startAndTerminateInstances(4);

        HazelcastInstance unknownNode = startNewInstance();
        assertTrue(getClusterService(unknownNode).isJoined());

        HazelcastInstance[] instances = restartInstances(addresses);
        assertEquals(0, instances.length);
    }

    @Test
    public void test_cannotChangeClusterState_beforeHotRestartProcessCompletes() {
        HazelcastInstance[] instances = startNewInstances(4);
        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        HazelcastInstance restartingInstance = instances[instances.length - 1];
        final Address address = getAddress(restartingInstance);
        restartingInstance.getLifecycleService().terminate();

        final CountDownLatch dataLoadLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                ClusterHotRestartEventListener listener = new ClusterHotRestartEventListener() {
                    @Override
                    public void onDataLoadStart(Address address) {
                        try {
                            dataLoadLatch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                restartInstance(address, listener);
            }
        });

        final CountDownLatch memberAddedLatch = new CountDownLatch(1);
        final HazelcastInstance masterInstance = instances[0];
        masterInstance.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAddedLatch.countDown();
            }
        });
        assertOpenEventually(memberAddedLatch);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                try {
                    masterInstance.getCluster().changeClusterState(ClusterState.ACTIVE);
                    fail("Should not be able to change cluster state when hot restart is not completed yet!");
                } catch (IllegalStateException expected) {
                    ignore(expected);
                }
            }
        }, 5);

        dataLoadLatch.countDown();
    }

    @Test
    public void test_backupReplicasAreSynced_whileShuttingDownCluster() {
        HazelcastInstance[] instances = startNewInstances(3);
        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, 100f);
        }

        final String mapName = mapNames[2];
        final int entryCount = 1000;

        IMap<Object, Object> map = instances[0].getMap(mapName);
        for (int i = 0; i < entryCount; i++) {
            map.setAsync(i, i);
        }

        final IMap<Object, Object> finalMap = map;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(entryCount, finalMap.size());
            }
        });

        Address[] addresses = getAddresses(instances);
        instances[0].getCluster().shutdown();

        instances = restartInstances(addresses);
        assertInstancesJoined(3, NodeState.ACTIVE, ClusterState.ACTIVE);

        map = instances[0].getMap(mapName);

        for (int i = 0; i < entryCount; i++) {
            assertEquals(i, map.get(i));
        }

        for (int i = 1; i < instances.length; i++) {
            instances[i].getLifecycleService().terminate();
        }

        for (int i = 0; i < entryCount; i++) {
            assertEquals(i, map.get(i));
        }
    }

    @Test
    public void test_backupOperationNotAllowed_untilStartupIsCompleted() throws Exception {
        Assume.assumeTrue("Restarting a single node, address change is irrelevant", addressChangePolicy == NONE);

        HazelcastInstance[] instances = startNewInstances(2);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);

        final Address restartingNodeAddress = getAddress(instances[1]);
        instances[1].shutdown();

        final CountDownLatch dataLoadStartedLatch = new CountDownLatch(1);
        final CountDownLatch dataLoadResumeLatch = new CountDownLatch(1);
        spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() throws Exception {
                return restartInstance(restartingNodeAddress, new ClusterHotRestartEventListener() {
                    @Override
                    public void onDataLoadStart(Address address) {
                        dataLoadStartedLatch.countDown();
                        assertOpenEventually(dataLoadResumeLatch);
                    }
                });
            }
        });

        assertClusterSizeEventually(2, instances[0]);
        assertOpenEventually(dataLoadStartedLatch);

        int partitionId = getPartitionId(instances[0]);
        InternalCompletableFuture<Object> future = getOperationService(instances[0])
                .createInvocationBuilder("", new PrimaryOperation(), partitionId)
                .setFailOnIndeterminateOperationState(true)
                .invoke();

        try {
            future.get();
            fail("Backups should not be allowed before startup is completed!");
        } catch (ExecutionException e) {
            assertInstanceOf(IndeterminateOperationStateException.class, e.getCause());
        }
        dataLoadResumeLatch.countDown();
    }

    @Test
    public void test_antiEntropyCheckNotAllowed_untilStartupIsCompleted() throws Exception {
        Assume.assumeTrue("Restarting a single node, address change is irrelevant", addressChangePolicy == NONE);

        final HazelcastInstance[] instances = startNewInstances(2);
        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);

        final Address restartingNodeAddress = getAddress(instances[1]);
        instances[1].shutdown();

        IMap<Object, Object> map = instances[0].getMap(mapNames[1]);
        String key = generateKeyOwnedBy(instances[0]);
        map.set(key, "value");

        final CountDownLatch dataLoadStartedLatch = new CountDownLatch(1);
        final CountDownLatch dataLoadResumeLatch = new CountDownLatch(1);
        spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() throws Exception {
                return restartInstance(restartingNodeAddress, new ClusterHotRestartEventListener() {
                    @Override
                    public void onDataLoadStart(Address address) {
                        dataLoadStartedLatch.countDown();
                        assertOpenEventually(dataLoadResumeLatch);
                    }
                });
            }
        });

        assertClusterSizeEventually(2, instances[0]);
        assertOpenEventually(dataLoadStartedLatch);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(isInstanceInSafeState(instances[0]));
            }
        }, 5);

        dataLoadResumeLatch.countDown();

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isInstanceInSafeState(instances[0]));
            }
        });
    }
}
