package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.ALL;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.NONE;
import static com.hazelcast.spi.hotrestart.cluster.AbstractHotRestartClusterStartTest.AddressChangePolicy.PARTIAL;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueAllTheTime;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterStartTest extends AbstractHotRestartClusterStartTest {

    @Parameterized.Parameters(name = "addressChangePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[] {NONE, PARTIAL, ALL});
    }

    @Test
    public void testSingleMemberRestart() throws IOException, InterruptedException {
        Address address = startAndTerminateInstance();
        HazelcastInstance hz = restartInstance(address);
        assertInstancesJoined(1, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(hz);
    }

    @Test
    public void test_hotRestart_whenClusterState_ACTIVE() throws IOException, InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        HazelcastInstance[] instances = restartInstances(addresses);
        assertInstancesJoined(4, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_hotRestart_withMigration() throws Exception{
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
    public void test_hotRestart_whenClusterState_FROZEN() throws IOException, InterruptedException {
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
    public void test_hotRestart_whenClusterState_PASSIVE() throws IOException, InterruptedException {
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
    public void test_hotRestartFails_withMissingHotRestartDirectory_forOneNode() throws IOException, InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        Address randomAddress = addresses[RandomPicker.getInt(addresses.length)];
        deleteHotRestartDirectoryOfNode(randomAddress);

        HazelcastInstance[] instances = restartInstances(addresses);
        if (instances.length == 1) {
            HazelcastInstance instance = instances[0];
            assertTrue(getNode(instance).joined());
            assertClusterSizeEventually(1, instance);
        } else {
            assertEquals(0, instances.length);
        }
    }

    @Test
    public void test_hotRestartFails_whenNodeStartsBeforeOthers_withMissingHotRestartDirectory()
            throws IOException, InterruptedException {

        Address[] addresses = startAndTerminateInstances(4);

        Address randomAddress = addresses[RandomPicker.getInt(addresses.length)];
        deleteHotRestartDirectoryOfNode(randomAddress);

        HazelcastInstance instance = restartInstance(randomAddress);
        assertTrue(getNode(instance).joined());

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
    public void test_hotRestartFails_withUnknownNode() throws IOException, InterruptedException {
        Address[] addresses = startAndTerminateInstances(4);

        HazelcastInstance unknownNode = startNewInstance();
        assertTrue(getNode(unknownNode).joined());

        HazelcastInstance[] instances = restartInstances(addresses);
        assertEquals(0, instances.length);
    }

    @Test
    public void test_cannotChangeClusterState_beforeHotRestartProcessCompletes()
            throws IOException, InterruptedException {

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
                }
            }
        }, 5);

        dataLoadLatch.countDown();
    }
}
