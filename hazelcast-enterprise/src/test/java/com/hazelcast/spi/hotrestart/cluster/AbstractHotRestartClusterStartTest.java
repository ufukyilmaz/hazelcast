package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.partition.PartitionLostListenerStressTest.EventCollectingPartitionLostListener;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;

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
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractHotRestartClusterStartTest {

    protected static final int PARTITION_COUNT = 50;

    private static final AtomicInteger instanceNameIndex = new AtomicInteger(0);

    public enum AddressChangePolicy {
        NONE, PARTIAL, ALL
    }

    protected final String[] mapNames = new String[]{"map0", "map1", "map2", "map3", "map4", "map5", "map6"};
    protected final String[] cacheNames = new String[]{"cache0", "cache1", "cache2", "cache3", "cache4", "cache5", "cache6"};

    protected final EventCollectingPartitionLostListener partitionLostListener = new EventCollectingPartitionLostListener();

    protected int validationTimeoutInSeconds = 10, dataLoadTimeoutInSeconds = 10;

    private Future startNodeFuture;

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameter
    public volatile AddressChangePolicy addressChangePolicy = AddressChangePolicy.NONE;

    protected File baseDir;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private ConcurrentMap<Address, String> instanceNames = new ConcurrentHashMap<Address, String>();

    @Before
    public void before() throws IOException {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        IOUtil.delete(baseDir);

        if (!baseDir.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
    }

    @After
    public void after() throws IOException {
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
        IOUtil.delete(baseDir);
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances) throws InterruptedException {
        return startNewInstances(numberOfInstances, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance[] startNewInstances(int numberOfInstances, final HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(numberOfInstances);

        for (int i = 0; i < numberOfInstances; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = startNewInstance(null, clusterStartPolicy);
                        instancesList.add(instance);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }

                }
            }).start();
        }

        assertOpenEventually(latch);

        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
    }

    HazelcastInstance[] restartInstances(Address[] addresses) throws InterruptedException {
        return restartInstances(addresses, Collections.<Address, ClusterHotRestartEventListener>emptyMap());
    }

    // restart instances at given codebase version
    HazelcastInstance[] restartInstances(Address[] addresses, MemberVersion version) throws InterruptedException {
        return restartInstances(addresses, Collections.<Address, ClusterHotRestartEventListener>emptyMap(),
                HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY, version);
    }

    HazelcastInstance[] restartInstances(Address[] addresses, Map<Address, ClusterHotRestartEventListener> listeners)
            throws InterruptedException {
        return restartInstances(addresses, listeners, HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
    }

    HazelcastInstance[] restartInstances(Address[] addresses, Map<Address, ClusterHotRestartEventListener> listeners,
            final HotRestartClusterDataRecoveryPolicy clusterStartPolicy)
            throws InterruptedException {

        return restartInstances(addresses, listeners, clusterStartPolicy, null);
    }

    HazelcastInstance[] restartInstances(Address[] addresses, Map<Address, ClusterHotRestartEventListener> listeners,
                                         final HotRestartClusterDataRecoveryPolicy clusterStartPolicy,
                                         final MemberVersion version)
            throws InterruptedException {

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
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }

                }
            }).start();
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

    Address startAndTerminateInstance() throws InterruptedException {
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

    Address[] startAndTerminateInstances(int numberOfInstances) throws InterruptedException {
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
        final EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(instance).getNodeExtension();
        return nodeExtension.getHotRestartService().getClusterMetadataManager().getHomeDir();
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
        config.setInstanceName(instanceName);

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
        IOUtil.delete(hotRestartDir);
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
            assertTrue("node: " + getAddress(instance), node.joined());
            assertEquals("node: " + getAddress(instance), expectedNodeState, node.getState());
            assertEquals("node: " + getAddress(instance), expectedClusterState, instance.getCluster().getClusterState());
        }
    }

    void assertInstancesJoinedEventually(final int numberOfInstances, final NodeState nodeState, final ClusterState clusterState) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
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
        startNodeFuture = spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    Address address = node.getThisAddress();
                    assertNodeStateEventually(node, NodeState.SHUT_DOWN);
                    // we can't change address anymore after cluster restart is initiated
                    addressChangePolicy = AddressChangePolicy.NONE;
                    restartInstance(address);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
