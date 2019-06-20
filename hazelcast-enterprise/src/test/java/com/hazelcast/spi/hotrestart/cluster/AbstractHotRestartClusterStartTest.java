package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public abstract class AbstractHotRestartClusterStartTest extends HazelcastTestSupport {

    protected static final int PARTITION_COUNT = 50;

    public enum ReuseAddress {
        /**
         * Reuse all previous addresses
         */
        ALWAYS,
        /**
         * Randomly decide whether or not to reuse an address.
         */
        SOMETIMES,
        /**
         * Never reuse, always pick new addresses
         */
        NEVER
    }

    private final ILogger logger = Logger.getLogger(getClass());

    protected final String[] mapNames = new String[]{"map0", "map1", "map2", "map3", "map4", "map5", "map6"};
    protected final String[] cacheNames = new String[]{"cache0", "cache1", "cache2", "cache3", "cache4", "cache5", "cache6"};

    protected final EventCollectingPartitionLostListener partitionLostListener = new EventCollectingPartitionLostListener();

    protected int validationTimeoutInSeconds = 60;
    protected int dataLoadTimeoutInSeconds = 60;

    private Future startNodeFuture;

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @Parameter
    public volatile ReuseAddress reuseAddress = ReuseAddress.ALWAYS;

    private File baseDir;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Before
    public void before() {
        baseDir = hotRestartFolderRule.getBaseDir();
    }

    @After
    public void after() {
        if (startNodeFuture != null) {
            try {
                // Nothing to do with test's correctness.
                // Just to avoid some IO errors to be printed
                // when test finishes and deletes hot-restart folders
                // before new node's restart process completes.
                startNodeFuture.get(2, TimeUnit.MINUTES);
            } catch (Exception ignored) {
            }
        }

        factory.terminateAll();
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances) {
        return startNewInstances(numberOfInstances, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        return startNewInstances(numberOfInstances, clusterStartPolicy, null);
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances,
                                          HotRestartClusterDataRecoveryPolicy clusterStartPolicy,
                                          MemberVersion codebaseVersion) {
        List<HazelcastInstance> instancesList = new ArrayList<HazelcastInstance>();
        for (int i = 0; i < numberOfInstances; i++) {
            HazelcastInstance instance = startNewInstance(null, clusterStartPolicy, codebaseVersion);
            instancesList.add(instance);
        }
        return instancesList.toArray(new HazelcastInstance[0]);
    }

    HazelcastInstance[] restartInstances(Address[] addresses) {
        return restartInstances(addresses, Collections.<Address, ClusterHotRestartEventListener>emptyMap());
    }

    // restart instances at given codebase version
    HazelcastInstance[] restartInstances(Address[] addresses, MemberVersion version) {
        return restartInstances(addresses, Collections.<Address, ClusterHotRestartEventListener>emptyMap(),
                HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY, version);
    }

    HazelcastInstance[] restartInstances(Address[] addresses, Map<Address, ClusterHotRestartEventListener> listeners) {
        return restartInstances(addresses, listeners, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance[] restartInstances(Address[] addresses, Map<Address, ClusterHotRestartEventListener> listeners,
                                         HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        return restartInstances(addresses, listeners, clusterStartPolicy, null);
    }

    HazelcastInstance[] restartInstances(Address[] addresses, Map<Address, ClusterHotRestartEventListener> listeners,
                                         final HotRestartClusterDataRecoveryPolicy clusterStartPolicy,
                                         final MemberVersion version) {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(addresses.length);

        for (final Address address : addresses) {
            final ClusterHotRestartEventListener listener = listeners.get(address);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = restartInstance(address, listener, clusterStartPolicy, version);
                        instancesList.add(instance);
                    } catch (Throwable e) {
                        logger.severe(e);
                    } finally {
                        latch.countDown();
                    }

                }
            }, "Restart thread for " + address).start();
        }

        assertOpenEventually(latch);

        return instancesList.toArray(new HazelcastInstance[0]);
    }

    HazelcastInstance startNewInstance() {
        return startNewInstance(null, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance startNewInstance(ClusterHotRestartEventListener listener,
                                       HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        return startNewInstance(listener, clusterStartPolicy, null);
    }

    HazelcastInstance startNewInstance(ClusterHotRestartEventListener listener,
                                       HotRestartClusterDataRecoveryPolicy clusterStartPolicy,
                                       MemberVersion codebaseVersion) {
        Address address = factory.nextAddress();
        Config config = newConfig(listener, clusterStartPolicy);
        if (codebaseVersion == null) {
            return factory.newHazelcastInstance(address, config);
        } else {
            String existingHazelcastVersionValue = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
            try {
                System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, codebaseVersion.toString());
                return factory.newHazelcastInstance(address, config);
            } finally {
                if (existingHazelcastVersionValue == null) {
                    System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
                } else {
                    System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, existingHazelcastVersionValue);
                }
            }
        }
    }

    HazelcastInstance restartInstance(Address address) {
        return restartInstance(address, null, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance restartInstance(Address address, MemberVersion codebaseVersion) {
        return restartInstance(address, null, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY, codebaseVersion);
    }

    HazelcastInstance restartInstance(Address address, ClusterHotRestartEventListener listener) {
        return restartInstance(address, listener, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance restartInstance(Address address, ClusterHotRestartEventListener listener,
                                      HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        return restartInstance(address, listener, clusterStartPolicy, null);
    }

    HazelcastInstance restartInstance(Address address, ClusterHotRestartEventListener listener,
                                      HotRestartClusterDataRecoveryPolicy clusterStartPolicy, MemberVersion codebaseVersion) {
        String existingHazelcastVersionValue = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        try {
            if (codebaseVersion != null) {
                System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, codebaseVersion.toString());
            }

            Config config = newConfig(listener, clusterStartPolicy);
            Address newAddress = getRestartingAddress(address);
            return factory.newHazelcastInstance(newAddress, config);
        } finally {
            if (existingHazelcastVersionValue == null) {
                System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
            } else {
                System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, existingHazelcastVersionValue);
            }
        }
    }

    private Address getRestartingAddress(Address address) {
        switch (reuseAddress) {
            case ALWAYS:
                return address;
            case NEVER:
                return factory.nextAddress();
            case SOMETIMES:
                return (address.getPort() % 2 == 1) ? factory.nextAddress() : address;
        }
        throw new IllegalStateException("Should not reach here!");
    }

    Address startAndTerminateInstance() {
        HazelcastInstance instance = startNewInstance();

        warmUpPartitions(instance);
        changeClusterStateEventually(instance, ClusterState.PASSIVE);

        File dir = getHotRestartClusterDir(instance);
        Address address = getAddress(instance);
        terminateInstances();

        ClusterStateWriter writer = new ClusterStateWriter(dir);
        try {
            writer.write(ClusterState.ACTIVE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return address;
    }

    Address[] startAndTerminateInstances(int numberOfInstances) {
        HazelcastInstance[] instances = startNewInstances(numberOfInstances);
        assertInstancesJoined(numberOfInstances, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        List<File> dirs = collectHotRestartClusterDirs(instances);

        Address[] addresses = getAddresses(instances);
        terminateInstances();

        for (File dir : dirs) {
            ClusterStateWriter writer = new ClusterStateWriter(dir);
            try {
                writer.write(ClusterState.ACTIVE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return addresses;
    }

    Address[] getAddresses(HazelcastInstance[] instances) {
        Address[] addresses = new Address[instances.length];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = getAddress(instances[i]);
        }
        return addresses;
    }

    private List<File> collectHotRestartClusterDirs(HazelcastInstance... instances) {
        List<File> dirs = new ArrayList<File>();
        for (HazelcastInstance instance : instances) {
            dirs.add(getHotRestartClusterDir(instance));
        }
        return dirs;
    }

    private File getHotRestartClusterDir(HazelcastInstance instance) {
        Node node = getNode(instance);
        ClusterMetadataManager clusterMetadataManager = getClusterMetadataManager(node);
        return clusterMetadataManager.getHomeDir();
    }

    protected ClusterMetadataManager getClusterMetadataManager(Node node) {
        return getHotRestartIntegrationService(node).getClusterMetadataManager();
    }

    protected HotRestartIntegrationService getHotRestartIntegrationService(Node node) {
        NodeExtension nodeExtension = node.getNodeExtension();
        return (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService();
    }

    void terminateInstances() {
        factory.terminateAll();
    }

    Collection<HazelcastInstance> getAllInstances() {
        return factory.getAllHazelcastInstances();
    }

    Config newConfig(ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT))
                .setProperty(GroupProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED.getName(), "true")
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        for (int i = 0; i < mapNames.length; i++) {
            MapConfig mapConfig = config.getMapConfig(mapNames[i])
                    .setBackupCount(i);
            mapConfig.getHotRestartConfig()
                    .setEnabled(true);

            CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheNames[i])
                    .setBackupCount(i);
            cacheConfig.getHotRestartConfig()
                    .setEnabled(true);
        }

        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(baseDir)
                .setClusterDataRecoveryPolicy(clusterStartPolicy)
                .setValidationTimeoutSeconds(validationTimeoutInSeconds)
                .setDataLoadTimeoutSeconds(dataLoadTimeoutInSeconds);

        config.addListenerConfig(new ListenerConfig(partitionLostListener));
        if (listener != null) {
            config.addListenerConfig(new ListenerConfig(listener));
        }

        return config;
    }

    void assertInstancesJoined(int numberOfInstances, NodeState nodeState, ClusterState clusterState) {
        Collection<HazelcastInstance> allHazelcastInstances = getAllInstances();
        HazelcastInstance[] instances = allHazelcastInstances.toArray(new HazelcastInstance[0]);
        assertInstancesJoined(numberOfInstances, instances, nodeState, clusterState);
    }

    static void assertInstancesJoined(int numberOfInstances, HazelcastInstance[] instances, NodeState expectedNodeState,
                                      ClusterState expectedClusterState) {
        assertInstancesJoined(numberOfInstances, Arrays.asList(instances), expectedNodeState, expectedClusterState);
    }

    static void assertInstancesJoined(int numberOfInstances, Collection<HazelcastInstance> instances, NodeState expectedNodeState,
                                      ClusterState expectedClusterState) {
        assertEqualsStringFormat("Expected %s instances in the cluster, but found %s", numberOfInstances, instances.size());
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            assertNotNull("node should not be null", node);
            assertTrue("node " + getAddress(instance) + " should be joined", node.getClusterService().isJoined());
            assertEquals("node " + getAddress(instance) + " should be in state " + expectedNodeState,
                    expectedNodeState, node.getState());
            assertEquals("node " + getAddress(instance) + " should be in cluster state " + expectedClusterState,
                    expectedClusterState, instance.getCluster().getClusterState());
        }
    }

    void assertInstancesJoinedEventually(final int numberOfInstances, final NodeState nodeState, final ClusterState clusterState) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Collection<HazelcastInstance> allHazelcastInstances = getAllInstances();
                HazelcastInstance[] instances = allHazelcastInstances.toArray(new HazelcastInstance[0]);
                assertInstancesJoined(numberOfInstances, instances, nodeState, clusterState);
            }
        });
    }

    static void assertNodeStateEventually(final Node node, final NodeState expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEqualsStringFormat("Node should be in state %s, but found state %s", expected, node.getState());
            }
        });
    }

    void invokeDummyOperationOnAllPartitions(HazelcastInstance... instances) {
        for (HazelcastInstance hz : instances) {
            NodeEngine nodeEngine = getNodeEngineImpl(hz);
            OperationService operationService = nodeEngine.getOperationService();
            int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
            InternalCompletableFuture[] futures = new InternalCompletableFuture[partitionCount];
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                Operation op = new DummyPartitionAwareOperation().setPartitionId(partitionId);
                futures[partitionId] = operationService.invokeOnPartition(op);
            }
            for (InternalCompletableFuture f : futures) {
                try {
                    f.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
    }

    void startNodeAfterTermination(final Node node) {
        final Address address = node.getThisAddress();
        startNodeFuture = spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    assertNodeStateEventually(node, NodeState.SHUT_DOWN);
                    // we can't change address anymore after cluster restart is initiated
                    reuseAddress = ReuseAddress.ALWAYS;
                    restartInstance(address);
                } catch (Throwable e) {
                    logger.severe("Restart for " + address + " failed!", e);
                }
            }
        });
    }

    HazelcastInstance startNewInstanceWithBaseDir(File tmpBaseDir) {
        File baseDirBackup = baseDir;
        try {
            baseDir = tmpBaseDir;
            return startNewInstance();
        } finally {
            baseDir = baseDirBackup;
        }
    }

    File getHotRestartHome(HazelcastInstance hz) {
        return getHotRestartIntegrationService(getNode(hz)).getHotRestartHome();
    }

    File getBaseDir() {
        return baseDir;
    }

    private static class DummyPartitionAwareOperation extends Operation implements AllowedDuringPassiveState {
        @Override
        public void run() throws Exception {
        }
    }
}
