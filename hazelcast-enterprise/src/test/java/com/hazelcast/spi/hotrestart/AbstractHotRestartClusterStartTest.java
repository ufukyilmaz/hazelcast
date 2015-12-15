package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractHotRestartClusterStartTest {

    private static final boolean USE_NETWORK = false;
    private static final AtomicInteger portCounter = new AtomicInteger(5701);
    private static InetAddress localAddress;

    @BeforeClass
    public static void setupClass() throws UnknownHostException {
        localAddress = InetAddress.getLocalHost();
    }

    protected static List<Integer> acquirePorts(int numberOfPorts) {
        final int start = portCounter.getAndAdd(numberOfPorts);
        List<Integer> ports = new ArrayList<Integer>(numberOfPorts);
        for (int i = 0; i < numberOfPorts; i++) {
            ports.add(start + i);
        }
        return ports;
    }

    protected static int acquirePort() {
        return portCounter.getAndIncrement();
    }

    @Rule
    public TestName testName = new TestName();

    protected File hotRestartDir;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() throws IOException {
        hotRestartDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        IOUtil.delete(hotRestartDir);

        if (!hotRestartDir.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
        if (USE_NETWORK) {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    @After
    public void after() throws IOException {
        if (factory != null) {
            factory.terminateAll();
        }
        if (USE_NETWORK) {
            HazelcastInstanceFactory.terminateAll();
        }
        IOUtil.delete(hotRestartDir);
    }

    protected HazelcastInstance[] startInstances(List<Integer> ports)
            throws InterruptedException {
        return startInstances(ports, Collections.<Integer, ClusterHotRestartEventListener>emptyMap());
    }

    protected HazelcastInstance[] startInstances(List<Integer> ports,
                                                        final Map<Integer, ClusterHotRestartEventListener> listeners)
            throws InterruptedException {

        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(ports.size());

        if (factory == null) {
            initializeFactory(ports);
        }

        Collection<Address> addresses = new ArrayList<Address>(ports.size());
        Collection<Address> knownAddresses = factory.getKnownAddresses();
        for (Integer port : ports) {
            Address address = newAddress(port);
            assertTrue(knownAddresses.contains(address));
            addresses.add(address);
        }

        for (final Address address : addresses) {
            final ClusterHotRestartEventListener listener = listeners.get(address.getPort());
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = newHazelcastInstance(address, listener);
                        instancesList.add(instance);
                    } catch (Exception e) {
                        System.err.println("Node failed to start: " + address + "-> " + e);
                    } finally {
                        latch.countDown();
                    }

                }
            }).start();
        }

        assertOpenEventually(latch);

        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
    }

    protected HazelcastInstance startInstance(int port) {
        assertNotNull(factory);

        Address address = newAddress(port);
        assertTrue(factory.getKnownAddresses().contains(address));

        return newHazelcastInstance(address, null);
    }

    private HazelcastInstance newHazelcastInstance(Address address, ClusterHotRestartEventListener listener) {

        Config config = newConfig(listener);
        if (USE_NETWORK) {
            NetworkConfig networkConfig = config.getNetworkConfig();
            networkConfig.setPort(address.getPort());
            networkConfig.setPortAutoIncrement(false);

            JoinConfig join = networkConfig.getJoin();
            join.getMulticastConfig().setEnabled(false);
            TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
            tcpIpConfig.setEnabled(true).clear();

            Collection<Address> addresses = factory.getKnownAddresses();
            for (Address addr : addresses) {
                tcpIpConfig.addMember("127.0.0.1:" + addr.getPort());
            }
            return Hazelcast.newHazelcastInstance(config);
        }
        return factory.newHazelcastInstance(address, config);
    }

    protected void initializeFactory(List<Integer> portsToDiscover) {
        assertNull(factory);

        Collection<Address> addresses = new ArrayList<Address>(portsToDiscover.size());
        for (Integer port : portsToDiscover) {
            addresses.add(newAddress(port));
        }
        factory = new TestHazelcastInstanceFactory(addresses);
    }

    private Address newAddress(int port) {
        return new Address("127.0.0.1", localAddress, port);
    }

    protected void startAndCrashInstances(final List<Integer> ports)
            throws InterruptedException {

        HazelcastInstance[] instances = startInstances(ports);
        assertInstancesJoined(ports.size(), instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        terminateInstances();
    }

    protected void terminateInstances() {
        factory.terminateAll();
        factory = null;
        if (USE_NETWORK) {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    private Config newConfig(ClusterHotRestartEventListener listener) {
        Config config = new Config();
        config.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);

        config.getHotRestartPersistenceConfig().setEnabled(true)
                .setBaseDir(hotRestartDir)
                .setValidationTimeoutSeconds(10).setDataLoadTimeoutSeconds(10);

        if (listener != null) {
            config.addListenerConfig(new ListenerConfig(listener));
        }

        return config;
    }

    protected static void assertInstancesJoined(int numberOfInstances, HazelcastInstance[] instances, NodeState nodeState,
                                                ClusterState clusterState) {
        assertEquals(numberOfInstances, instances.length);
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            assertNotNull(node);
            assertTrue(node.joined());
            assertEquals(node.getState(), nodeState);
            assertEquals(instance.getCluster().getClusterState(), clusterState);
        }
    }

    protected void assertInstancesJoined(int numberOfInstances, NodeState nodeState, ClusterState clusterState) {
        Collection<HazelcastInstance> allHazelcastInstances = USE_NETWORK
                ? HazelcastInstanceFactory.getAllHazelcastInstances()
                : factory.getAllHazelcastInstances();
        HazelcastInstance[] instances = allHazelcastInstances.toArray(new HazelcastInstance[allHazelcastInstances.size()]);
        assertInstancesJoined(numberOfInstances, instances, nodeState, clusterState);
    }

    protected static void assertNodeStateEventually(final Node node, final NodeState expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, node.getState());
            }
        });
    }
}
