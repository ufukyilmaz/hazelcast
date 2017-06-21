package com.hazelcast.wan.discovery;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.wan.map.MapWanReplicationTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SlowTest.class)
public class WanDiscoveryTest extends MapWanReplicationTestSupport {
    private ArrayListDiscoveryStrategyFactory discoveryStrategyFactory;
    private String wanReplicationName;

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Override
    protected String getClusterEndPoints(Config config, int count) {
        return null;
    }

    @Before
    public void init() throws UnknownHostException {
        this.wanReplicationName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationName, PassThroughMergePolicy.class.getName());
        this.discoveryStrategyFactory = new ArrayListDiscoveryStrategyFactory();
        final DiscoveryStrategyConfig discoveryConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory);
        discoveryConfig.addProperty(WanReplicationProperties.DISCOVERY_PERIOD.key(), 1);
        addDiscoveryConfig(configA, wanReplicationName, discoveryConfig);
        discoveryStrategyFactory.strategy.nodes.addAll(clusterDiscoveryNodes(configB, clusterB.length));
    }

    @Test
    public void noDiscoveredNodesDoesNotThrowException() {
        final ArrayList<DiscoveryNode> discoveryStrategyList = discoveryStrategyFactory.strategy.nodes;
        final ArrayList<DiscoveryNode> discoveryStrategyListCopy = new ArrayList<DiscoveryNode>(discoveryStrategyList);

        discoveryStrategyList.clear();
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);

        discoveryStrategyList.add(discoveryStrategyListCopy.get(0));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(1);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void removeUnreachableEndpoint() throws UnknownHostException {
        final ArrayList<DiscoveryNode> discoveryStrategyList = discoveryStrategyFactory.strategy.nodes;
        final SimpleDiscoveryNode unreachableEndpoint = new SimpleDiscoveryNode(new Address("1.2.3.4", 1234));
        discoveryStrategyList.clear();
        discoveryStrategyList.add(unreachableEndpoint);

        startClusterA();

        createDataIn(clusterA, "map", 0, 1000);


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(1);
            }
        });
        discoveryStrategyList.clear();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, getTargetEndpoints().size());
            }
        });
    }

    @Test
    public void newNodeDiscoveredTest() {
        final DiscoveryNode removed = discoveryStrategyFactory.strategy.nodes.remove(0);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(1);

        discoveryStrategyFactory.strategy.nodes.add(removed);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(2);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFrom(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void previouslyDiscoveredNodeDisappearsAndIsRemoved() throws Exception {
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(2);

        discoveryStrategyFactory.strategy.nodes.remove(0);
        assertEquals(1, discoveryStrategyFactory.strategy.nodes.size());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(1);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFrom(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void shutdownAndRestartNode() throws Exception {
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(2);

        clusterB[1].shutdown();

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFrom(new HazelcastInstance[]{clusterB[0]}, "map2", 0, 1000, clusterA);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTargetEndpointSize(1);
            }
        });

        clusterB[1] = HazelcastInstanceFactory.newHazelcastInstance(configB.setInstanceName("newInstance"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertAllEndpointsDiscovered(2);
            }
        });

        createDataIn(clusterA, "map3", 0, 1000);
        assertDataInFrom(clusterB, "map3", 0, 1000, clusterA);
    }

    @Test
    public void testMaxConnected() {
        final WanReplicationConfig c = configA.getWanReplicationConfig(wanReplicationName);
        final WanPublisherConfig publisherConfig = c.getWanPublisherConfigs().iterator().next();
        publisherConfig.getProperties().put(WanReplicationProperties.MAX_ENDPOINTS.key(), 1);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        final ArrayList<DiscoveryNode> nodes = discoveryStrategyFactory.strategy.nodes;
        assertTargetEndpointSize(1);
        assertEquals(2, nodes.size());

        final Address connectedEndpoint = getTargetEndpoints().get(0);
        boolean connectedEndpointIsDiscovered = false;
        for (DiscoveryNode node : nodes) {
            connectedEndpointIsDiscovered |= node.getPublicAddress().equals(connectedEndpoint);
        }
        assertTrue(connectedEndpointIsDiscovered);
    }

    private void assertTargetEndpointSize(int expectedSize) {
        assertEquals(expectedSize, getTargetEndpoints().size());
    }

    private List<Address> getTargetEndpoints() {
        final EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(clusterA[0]);
        final WanBatchReplication endpoint = (WanBatchReplication) wanReplicationService.getEndpoint(
                wanReplicationName, configB.getGroupConfig().getName());
        return endpoint.getTargetEndpoints();
    }


    private void assertAllEndpointsDiscovered(int expectedSize) {
        final ArrayList<DiscoveryNode> nodes = discoveryStrategyFactory.strategy.nodes;
        final List<Address> targetEndpoints = getTargetEndpoints();
        assertEquals(expectedSize, targetEndpoints.size());
        assertEquals(nodes.size(), targetEndpoints.size());
        for (DiscoveryNode node : nodes) {
            targetEndpoints.contains(node.getPublicAddress());
        }
    }

    private static ArrayList<DiscoveryNode> clusterDiscoveryNodes(Config config, int count) throws UnknownHostException {
        final ArrayList<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>();
        int port = config.getNetworkConfig().getPort();
        for (int i = 0; i < count; i++) {
            final Address addr = new Address("127.0.0.1", port++);
            nodes.add(new SimpleDiscoveryNode(addr, addr));
        }
        return nodes;
    }

    private void addDiscoveryConfig(Config config, String setupName, DiscoveryStrategyConfig discoveryStrategyConfig) {
        final WanReplicationConfig c = config.getWanReplicationConfig(setupName);
        final WanPublisherConfig publisherConfig = c.getWanPublisherConfigs().iterator().next();
        final DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.addDiscoveryStrategyConfig(discoveryStrategyConfig);
        publisherConfig.setDiscoveryConfig(discoveryConfig);
    }

    public static class ArrayListDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        private ArrayListDiscoveryStrategy strategy = new ArrayListDiscoveryStrategy(null, Collections.<String, Comparable>emptyMap());

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return ArrayListDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            return strategy;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }

    public static class ArrayListDiscoveryStrategy extends AbstractDiscoveryStrategy {

        private final ArrayList<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>();

        ArrayListDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return nodes;
        }
    }
}
