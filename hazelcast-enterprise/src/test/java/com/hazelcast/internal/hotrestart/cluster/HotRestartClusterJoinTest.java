package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.impl.FrozenPartitionTableTest.NonRetryablePartitionOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.nio.IOUtil.toFileName;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static java.util.Collections.synchronizedList;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests in this class assumes {@link com.hazelcast.config.HotRestartPersistenceConfig#autoRemoveStaleData} is disabled.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartClusterJoinTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(HotRestartClusterJoinTest.class);

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Test
    public void hotRestart_fails_when_members_join_unknown_master() {
        Address unknownAddress = factory.nextAddress();

        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // start and shutdown cluster
        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        shutdownCluster(instances[0]);

        // start an unknown member
        HazelcastInstance hz = factory.newHazelcastInstance(unknownAddress, newConfig(unknownAddress));

        // restart previous members
        instances = startInstances(addresses);

        // hot restart should fail
        assertThat(instances, emptyArray());
        assertClusterSize(1, hz);
    }

    @Test
    public void hotRestart_fails_when_stale_members_join_master() {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // shutdown cluster
        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        shutdownCluster(instances);

        // partial start and shutdown single member
        Address masterAddress = addresses[0];
        HazelcastInstance hz = factory.newHazelcastInstance(masterAddress, newConfig(masterAddress));
        shutdownCluster(hz);

        // restart single master member
        hz = factory.newHazelcastInstance(masterAddress, newConfig(masterAddress));

        // restart remaining members
        Address[] newAddresses = new Address[addresses.length - 1];
        System.arraycopy(addresses, 1, newAddresses, 0, newAddresses.length);
        instances = startInstances(newAddresses);

        // hot restart should fail
        assertThat(instances, emptyArray());
        assertClusterSize(1, hz);
    }

    @Test
    public void hotRestart_with_unknown_member() {
        Address unknownAddress = factory.nextAddress();

        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];
        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // shutdown cluster
        assertClusterSizeEventually(instances);
        shutdownCluster(instances);

        Address[] newAddresses = Arrays.copyOf(addresses, addresses.length + 1);
        newAddresses[addresses.length] = unknownAddress;

        // restart cluster with an additional unknown member
        instances = startInstances(newAddresses);

        if (instances.length == 1) {
            // If unknown member starts up first and becomes master,
            // then hot restart should fail, only unknown member will survive.
            assertEquals(unknownAddress, getAddress(instances[0]));
        } else {
            assertThat(instances, arrayWithSize(newAddresses.length));
            Member[] members = instances[0].getCluster().getMembers().toArray(new Member[0]);
            // If unknown member starts during hot restart process,
            // its join request will be delayed until hot restart completes
            // and it will be allowed to join as the last member.
            assertEquals(unknownAddress, members[members.length - 1].getAddress());
        }
    }

    @Test
    public void hotRestart_with_stale_member() {
        HazelcastInstance[] instances = new HazelcastInstance[3];
        Address[] addresses = new Address[instances.length];

        // start cluster
        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig(address));
        }

        // shutdown cluster
        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        shutdownCluster(instances);

        // partial start cluster excluding single member
        Address excludedAddress = addresses[1];
        instances = startInstances(new Address[]{addresses[0], addresses[2]});

        // start an additional new member
        Address newMemberAddress = factory.nextAddress();
        HazelcastInstance hz = factory.newHazelcastInstance(newMemberAddress, newConfig(newMemberAddress));

        // shutdown cluster
        assertClusterSizeEventually(instances.length + 1, instances);
        assertClusterSizeEventually(instances.length + 1, hz);
        shutdownCluster(instances);

        // restart cluster with all members
        Address[] allAddresses = Arrays.copyOf(addresses, addresses.length + 1);
        allAddresses[addresses.length] = newMemberAddress;
        instances = startInstances(allAddresses);

        if (instances.length == 1) {
            // If excluded member starts up first and becomes master,
            // then only excluded member will survive.
            assertEquals(excludedAddress, getAddress(instances[0]));
        } else {
            // otherwise excluded member will fail, others will restart successfully.
            assertThat(instances, arrayWithSize(addresses.length));
            for (HazelcastInstance instance : instances) {
                assertNotEquals(excludedAddress, getAddress(instance));
            }
        }
    }

    @Test
    public void hotRestart_twoMembers_withSwappingAddresses() throws Exception {
        hotRestart_withSwappingAddresses(2);
    }

    @Test
    public void hotRestart_threeMembers_withSwappingAddresses() throws Exception {
        hotRestart_withSwappingAddresses(3);
    }

    private void hotRestart_withSwappingAddresses(int nodeCount) throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        final Address[] addresses = new Address[instances.length];
        final File[] hotRestartDirs = new File[instances.length];

        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
            hotRestartDirs[i] = getHotRestartHome(instances[i]);
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        PartitionTableView originalPartitionTable = getPartitionService(instances[0]).createPartitionTableView();

        shutdownCluster(instances);

        final Address[] newAddresses = new Address[addresses.length];
        System.arraycopy(addresses, 1, newAddresses, 0, addresses.length - 1);
        newAddresses[addresses.length - 1] = addresses[0];

        Future<HazelcastInstance>[] futures = new Future[addresses.length];
        for (int i = 0; i < nodeCount; i++) {
            final int ix = i;
            futures[i] = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return factory.newHazelcastInstance(newAddresses[ix], newConfig(hotRestartDirs[ix]));
                }
            });
        }
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
        assertClusterSizeEventually(instances);

        PartitionTableView newPartitionTable = getPartitionService(instances[0]).createPartitionTableView();
        assertEquals(originalPartitionTable.getLength(), newPartitionTable.getLength());

        for (int i = 0; i < originalPartitionTable.getLength(); i++) {
            for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                PartitionReplica originalReplica = originalPartitionTable.getReplica(i, j);
                PartitionReplica newReplica = newPartitionTable.getReplica(i, j);
                if (originalReplica != null) {
                    assertNotNull(newReplica);
                    assertNotEquals(originalReplica.address(), newReplica.address());
                    assertEquals(originalReplica.uuid(), newReplica.uuid());
                } else {
                    assertNull(newReplica);
                }
            }
        }

        OperationService operationService = getOperationService(instances[0]);
        for (int i = 0; i < newPartitionTable.getLength(); i++) {
            operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), i).join();
        }
    }

    @Test
    public void rollingRestart_withSwappingAddresses() throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[3];
        final Address[] addresses = new Address[instances.length];

        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        PartitionTableView originalPartitionTable = getPartitionService(instances[0]).createPartitionTableView();

        changeClusterStateEventually(instances[0], ClusterState.FROZEN);
        final File hotRestartHome1 = getHotRestartHome(instances[1]);
        final File hotRestartHome2 = getHotRestartHome(instances[2]);
        instances[1].shutdown();
        instances[2].shutdown();

        Future<HazelcastInstance>[] futures = new Future[2];
        futures[0] = spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() {
                return factory.newHazelcastInstance(addresses[1], newConfig(hotRestartHome2));
            }
        });
        futures[1] = spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() {
                return factory.newHazelcastInstance(addresses[2], newConfig(hotRestartHome1));
            }
        });

        instances[1] = futures[0].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        instances[2] = futures[1].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertClusterSizeEventually(instances);

        PartitionTableView newPartitionTable = getPartitionService(instances[0]).createPartitionTableView();
        assertEquals(originalPartitionTable.getLength(), newPartitionTable.getLength());

        for (int i = 0; i < originalPartitionTable.getLength(); i++) {
            for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                PartitionReplica originalReplica = originalPartitionTable.getReplica(i, j);
                PartitionReplica newReplica = newPartitionTable.getReplica(i, j);
                if (originalReplica != null) {
                    assertNotNull(newReplica);
                    assertEquals(originalReplica.uuid(), newReplica.uuid());
                    if (!addresses[0].equals(originalReplica.address())) {
                        assertNotEquals(originalReplica.address(), newReplica.address());
                    }
                } else {
                    assertNull(newReplica);
                }
            }
        }

        OperationService operationService = getOperationService(instances[0]);
        for (int i = 0; i < newPartitionTable.getLength(); i++) {
            operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), i).join();
        }
    }

    @Test
    public void partialRollingRestart_withSwappingAddresses() throws Exception {
        final HazelcastInstance[] instances = new HazelcastInstance[3];
        final Address[] addresses = new Address[instances.length];

        for (int i = 0; i < instances.length; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
            instances[i] = factory.newHazelcastInstance(address, newConfig());
        }

        assertClusterSizeEventually(instances);
        warmUpPartitions(instances);
        PartitionTableView originalPartitionTable = getPartitionService(instances[0]).createPartitionTableView();

        changeClusterStateEventually(instances[0], ClusterState.FROZEN);

        UUID missingMemberUuid = getClusterService(instances[1]).getLocalMember().getUuid();
        final File hotRestartHome = getHotRestartHome(instances[2]);
        instances[1].shutdown();
        instances[2].shutdown();

        Future<HazelcastInstance> future = spawn(new Callable<HazelcastInstance>() {
            @Override
            public HazelcastInstance call() {
                return factory.newHazelcastInstance(addresses[1], newConfig(hotRestartHome));
            }
        });

        instances[1] = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertClusterSizeEventually(2, instances[1]);

        PartitionTableView newPartitionTable0 = getPartitionService(instances[0]).createPartitionTableView();
        PartitionTableView newPartitionTable1 = getPartitionService(instances[1]).createPartitionTableView();

        for (int i = 0; i < originalPartitionTable.getLength(); i++) {
            for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                PartitionReplica originalReplica = originalPartitionTable.getReplica(i, j);
                PartitionReplica newReplica0 = newPartitionTable0.getReplica(i, j);
                PartitionReplica newReplica1 = newPartitionTable1.getReplica(i, j);
                if (originalReplica != null) {
                    assertNotNull(newReplica0);
                    assertNotNull(newReplica1);
                    assertEquals(originalReplica.uuid(), newReplica0.uuid());
                    assertEquals(originalReplica.uuid(), newReplica1.uuid());
                    assertEquals(newReplica0, newReplica1);
                } else {
                    assertNull(newReplica0);
                    assertNull(newReplica1);
                }
            }
        }

        OperationService operationService = getOperationService(instances[0]);
        for (int i = 0; i < newPartitionTable0.getLength(); i++) {
            InternalCompletableFuture<Object> f =
                    operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), i);

            PartitionReplica replica = newPartitionTable0.getReplica(i, 0);
            if (missingMemberUuid.equals(replica.uuid())) {
                try {
                    f.joinInternal();
                    fail("Invocation to missing member should have failed!");
                } catch (TargetNotMemberException ignored) {
                }
            } else {
                f.joinInternal();
            }
        }
    }

    private void assertClusterSizeEventually(HazelcastInstance... instances) {
        assertClusterSizeEventually(instances.length, instances);
    }

    private HazelcastInstance[] startInstances(Address[] addresses) {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(addresses.length);
        final Map<Address, Object> result = new ConcurrentHashMap<>();
        for (final Address address : addresses) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = factory.newHazelcastInstance(address, newConfig(address));
                        instancesList.add(instance);
                        result.put(address, Boolean.TRUE);
                    } catch (Throwable e) {
                        StringWriter sw = new StringWriter();
                        e.printStackTrace(new PrintWriter(sw));
                        result.put(address, sw.toString());
                    } finally {
                        latch.countDown();
                    }
                }
            }).start();
        }
        assertOpenEventually(latch);
        LOGGER.info("Started instances: " + result);
        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
    }

    private File getHotRestartHome(HazelcastInstance hz) {
        NodeExtension nodeExtension = getNode(hz).getNodeExtension();
        HotRestartIntegrationService service = (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService();
        return service.getHotRestartHome();
    }

    private Config newConfig() {
        return newConfig(hotRestartFolderRule.getBaseDir());
    }

    private Config newConfig(Address address) {
        String child = toFileName(address.getHost() + ":" + address.getPort());
        return newConfig(new File(hotRestartFolderRule.getBaseDir(), child));
    }

    private static Config newConfig(File baseDir) {
        Config config = new Config()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(baseDir)
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE)
                .setValidationTimeoutSeconds(10)
                .setDataLoadTimeoutSeconds(10)
                .setAutoRemoveStaleData(false);

        return config;
    }

    private void shutdownCluster(HazelcastInstance... instances) {
        assertThat(instances, not(emptyArray()));
        waitAllForSafeState(instances);
        instances[0].getCluster().shutdown();
    }
}
