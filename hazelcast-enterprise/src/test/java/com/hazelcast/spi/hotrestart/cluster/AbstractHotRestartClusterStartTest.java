package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public abstract class AbstractHotRestartClusterStartTest extends HazelcastTestSupport {

    protected static final int PARTITION_COUNT = 50;

    private static final AtomicInteger instanceNameIndex = new AtomicInteger(0);

    public enum AddressChangePolicy {
        NONE, PARTIAL, ALL
    }

    private final ILogger logger = Logger.getLogger(getClass());
    
    protected final String[] mapNames = new String[]{"map0", "map1", "map2", "map3", "map4", "map5", "map6"};
    protected final String[] cacheNames = new String[]{"cache0", "cache1", "cache2", "cache3", "cache4", "cache5", "cache6"};

    protected final EventCollectingPartitionLostListener partitionLostListener = new EventCollectingPartitionLostListener();

    protected int validationTimeoutInSeconds = 10, dataLoadTimeoutInSeconds = 10;

    private Future startNodeFuture;

    @Rule
    public TestName testName = new TestName();

    @Parameter
    public volatile AddressChangePolicy addressChangePolicy = AddressChangePolicy.NONE;

    protected File baseDir;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private ConcurrentMap<Address, String> instanceNames = new ConcurrentHashMap<Address, String>();

    @Before
    public void before() {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(baseDir);

        if (!baseDir.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
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
        delete(baseDir);
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances) {
        return startNewInstances(numberOfInstances, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances, final HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        List<HazelcastInstance> instancesList = new ArrayList<HazelcastInstance>();
        for (int i = 0; i < numberOfInstances; i++) {
            HazelcastInstance instance = startNewInstance(null, clusterStartPolicy);
            instancesList.add(instance);
        }
        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
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
                                         final HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
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

        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
    }

    HazelcastInstance startNewInstance() {
        return startNewInstance(null, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance startNewInstance(ClusterHotRestartEventListener listener,
                                       HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        Address address = factory.nextAddress();

        String instanceName = "instance_" + instanceNameIndex.getAndIncrement();
        assertNull(instanceNames.putIfAbsent(address, instanceName));

        Config config = newConfig(instanceName, listener, clusterStartPolicy);
        return factory.newHazelcastInstance(address, config);
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
            String instanceName = instanceNames.get(address);
            assertNotNull(instanceName);

            Config config = newConfig(instanceName, listener, clusterStartPolicy);

            Address newAddress = getRestartingAddress(address);
            if (!address.equals(newAddress)) {
                assertNull(instanceNames.putIfAbsent(newAddress, instanceName));
            }
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
        switch (addressChangePolicy) {
            case NONE:
                return address;
            case ALL:
                return factory.nextAddress();
            case PARTIAL:
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
            final ClusterStateWriter writer = new ClusterStateWriter(dir);
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
        final HotRestartIntegrationService hotRestartService =
                (HotRestartIntegrationService) getNode(instance).getNodeExtension().getInternalHotRestartService();
        return hotRestartService.getClusterMetadataManager().getHomeDir();
    }

    void terminateInstances() {
        factory.terminateAll();
    }

    Collection<HazelcastInstance> getAllInstances() {
        return factory.getAllHazelcastInstances();
    }

    Config newConfig(String instanceName, ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        Config config = new Config();

        for (int i = 0; i < mapNames.length; i++) {
            MapConfig mapConfig = config.getMapConfig(mapNames[i]);
            mapConfig.setBackupCount(i);
            mapConfig.getHotRestartConfig().setEnabled(true);
            CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheNames[i]);
            cacheConfig.setBackupCount(i);
            cacheConfig.getHotRestartConfig().setEnabled(true);
        }

        config.addListenerConfig(new ListenerConfig(partitionLostListener));

        config.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(new File(baseDir, instanceName))
                .setClusterDataRecoveryPolicy(clusterStartPolicy)
                .setValidationTimeoutSeconds(validationTimeoutInSeconds)
                .setDataLoadTimeoutSeconds(dataLoadTimeoutInSeconds);

        if (listener != null) {
            config.addListenerConfig(new ListenerConfig(listener));
        }

        config.setProperty(GroupProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED.getName(), "true");

        return config;
    }

    void deleteHotRestartDirectoryOfNode(Address address) {
        String hotRestartDirName = instanceNames.get(address);
        assertNotNull(hotRestartDirName);

        File hotRestartDir = new File(baseDir, hotRestartDirName);
        assertTrue("HotRestart dir doesn't exist: " + hotRestartDir.getAbsolutePath(), hotRestartDir.exists());
        delete(hotRestartDir);
    }

    void assertInstancesJoined(int numberOfInstances, NodeState nodeState, ClusterState clusterState) {
        Collection<HazelcastInstance> allHazelcastInstances = getAllInstances();
        HazelcastInstance[] instances = allHazelcastInstances.toArray(new HazelcastInstance[allHazelcastInstances.size()]);
        assertInstancesJoined(numberOfInstances, instances, nodeState, clusterState);
    }

    static void assertInstancesJoined(int numberOfInstances, HazelcastInstance[] instances, NodeState expectedNodeState,
                                      ClusterState expectedClusterState) {
        assertInstancesJoined(numberOfInstances, Arrays.asList(instances), expectedNodeState, expectedClusterState);
    }

    static void assertInstancesJoined(int numberOfInstances, Collection<HazelcastInstance> instances, NodeState expectedNodeState,
                                      ClusterState expectedClusterState) {
        assertEquals(numberOfInstances, instances.size());
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            assertNotNull(node);
            assertTrue("node: " + getAddress(instance), node.getClusterService().isJoined());
            assertEquals("node: " + getAddress(instance), expectedNodeState, node.getState());
            assertEquals("node: " + getAddress(instance), expectedClusterState, instance.getCluster().getClusterState());
        }
    }

    void assertInstancesJoinedEventually(final int numberOfInstances, final NodeState nodeState, final ClusterState clusterState) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> allHazelcastInstances = getAllInstances();
                HazelcastInstance[] instances = allHazelcastInstances.toArray(new HazelcastInstance[allHazelcastInstances.size()]);
                assertInstancesJoined(numberOfInstances, instances, nodeState, clusterState);
            }
        });
    }

    static void assertNodeStateEventually(final Node node, final NodeState expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, node.getState());
            }
        });
    }

    void invokeDummyOperationOnAllPartitions(HazelcastInstance... instances) {
        if (addressChangePolicy == AddressChangePolicy.NONE) {
            // not needed
            return;
        }

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
                    f.get(2, TimeUnit.MINUTES);
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
    }

    private static class DummyPartitionAwareOperation extends Operation implements AllowedDuringPassiveState {
        @Override
        public void run() throws Exception {
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
                    addressChangePolicy = AddressChangePolicy.NONE;
                    restartInstance(address);
                } catch (Throwable e) {
                    logger.severe("Restart for " + address + " failed!", e);
                }
            }
        });
    }
}
