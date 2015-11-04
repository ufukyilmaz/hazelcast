package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.hotrestart.cluster.ClusterHotRestartEventListener;
import com.hazelcast.test.AssertTask;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractHotRestartClusterStartTest {

    static final String HOT_RESTART_DIRECTORY = "hot-restart-cluster-test";

    @Before
    public void before() throws IOException {
        HazelcastInstanceFactory.terminateAll();
        clearHotRestartFolder();

        File hotRestartFolder = new File(HOT_RESTART_DIRECTORY);
        if (!hotRestartFolder.mkdir()) {
            throw new IllegalStateException("Failed to create hot-restart directory!");
        }
    }

    @After
    public void after() throws IOException {
        HazelcastInstanceFactory.terminateAll();
        clearHotRestartFolder();
    }

    protected void clearHotRestartFolder() throws IOException {
        IOUtil.delete(new File(HOT_RESTART_DIRECTORY));
    }

    protected static HazelcastInstance[] startInstances(final List<Integer> portsToStart, List<Integer> portsToDiscover)
            throws InterruptedException {
        return startInstances(portsToStart, portsToDiscover, Collections.<Integer, ClusterHotRestartEventListener>emptyMap());
    }

    protected static HazelcastInstance[] startInstances(final List<Integer> portsToStart, List<Integer> portsToDiscover,
                                                        Map<Integer, ClusterHotRestartEventListener> listeners)
            throws InterruptedException {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(portsToStart.size());

        for (final int port : portsToStart) {
            final Config config = newConfig(port, portsToDiscover, listeners.get(port));
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
                        instancesList.add(instance);
                    } catch (Exception e) {
                        System.err.println("Node failed to start: " + port + "-> " + e);
                    } finally {
                        latch.countDown();
                    }

                }
            }).start();
        }

        assertOpenEventually(latch);

        return instancesList.toArray(new HazelcastInstance[instancesList.size()]);
    }

    protected void startAndCrashInstances(final List<Integer> ports)
            throws InterruptedException {
        HazelcastInstance[] instances = startInstances(ports, ports);

        assertInstancesJoined(ports, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        HazelcastInstanceFactory.terminateAll();
    }

    protected static Config newConfig(int nodePort, List<Integer> ports, ClusterHotRestartEventListener listener) {
        Config config = new Config();
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN, "0");

        config.getHotRestartConfig().setEnabled(true)
                .setHomeDir(new File(HOT_RESTART_DIRECTORY))
                .setValidationTimeoutSeconds(10).setDataLoadTimeoutSeconds(10);

        final NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(nodePort);
        networkConfig.setPortAutoIncrement(false);
        final JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);

        final TcpIpConfig tcpIpConfig = join.getTcpIpConfig().clear().setEnabled(true);
        for (int port : ports) {
            tcpIpConfig.addMember("127.0.0.1:" + port);
        }

        if (listener != null) {
            config.addListenerConfig(new ListenerConfig(listener));
        }

        return config;
    }
    protected static void assertInstancesJoined(List<Integer> ports, HazelcastInstance[] instances, NodeState nodeState,
                                                ClusterState clusterState) {
        assertEquals(ports.size(), instances.length);
        for (HazelcastInstance instance : instances) {
            final Node node = getNode(instance);
            assertTrue(node.joined());
            assertEquals(node.getState(), nodeState);
            assertEquals(instance.getCluster().getClusterState(), clusterState);
        }
    }

    protected static void assertInstancesJoined(List<Integer> ports, NodeState nodeState, ClusterState clusterState) {
        HazelcastInstance[] instances = Hazelcast.getAllHazelcastInstances().toArray(new HazelcastInstance[]{});
        assertInstancesJoined(ports, instances, nodeState, clusterState);
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
