package com.hazelcast.wan.discovery;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.impl.replication.WanReplicationProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.wan.map.MapWanReplicationTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanDiscoveryTest extends MapWanReplicationTestSupport {

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

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
    public void init() {
        super.init();
        this.wanReplicationName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationName, PassThroughMergePolicy.class.getName());
    }

    @Test
    public void recoversFromExceptionThrowingStrategy() {
        final ExceptionThrowingDiscoveryStrategyFactory factory
                = setupDiscoveryStrategyFactory(new ExceptionThrowingDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length));
        factory.strategy.throwException = true;

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);

        factory.strategy.throwException = false;

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(discoveryEndpoints, clusterB.length);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);
        assertDataInFromEventually(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void noDiscoveredNodesDoesNotThrowException() {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> endpoints = clusterDiscoveryNodes(configB, clusterB.length);
        final ArrayList<DiscoveryNode> strategyEndpoints = factory.strategy.nodes;

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);

        strategyEndpoints.add(endpoints.get(0));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(strategyEndpoints, 1);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);
        assertDataInFromEventually(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void removeUnreachableEndpoint() throws UnknownHostException {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> strategyEndpoints = factory.strategy.nodes;

        final SimpleDiscoveryNode unreachableEndpoint = new SimpleDiscoveryNode(new Address("1.2.3.4", 1234));
        strategyEndpoints.add(unreachableEndpoint);

        startClusterA();

        createDataIn(clusterA, "map", 0, 1000);


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(strategyEndpoints, 1);
            }
        });
        strategyEndpoints.clear();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, getTargetEndpoints().size());
            }
        });
    }

    @Test
    public void newNodeDiscoveredTest() {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length));
        final DiscoveryNode removed = discoveryEndpoints.remove(0);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(discoveryEndpoints, 1);

        discoveryEndpoints.add(removed);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(discoveryEndpoints, 2);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFromEventually(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void previouslyDiscoveredNodeDisappearsAndIsRemoved() {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length));

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(discoveryEndpoints, 2);

        discoveryEndpoints.remove(0);
        assertEquals(1, discoveryEndpoints.size());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertAllEndpointsDiscovered(discoveryEndpoints, 1);
            }
        });

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFromEventually(clusterB, "map2", 0, 1000, clusterA);
    }

    @Test
    public void shutdownAndRestartNode() {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length));

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(discoveryEndpoints, 2);

        clusterB[1].shutdown();

        createDataIn(clusterA, "map2", 0, 1000);
        assertDataInFromEventually(new HazelcastInstance[]{clusterB[0]}, "map2", 0, 1000, clusterA);

        // faulty address must have been removed because we could not replicate to it
        // but it will be readded as a result of periodic WAN discovery implementation
        // returning two endpoints
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTargetEndpointSize(2);
            }
        });

        clusterB[1] = super.factory.newHazelcastInstance(configB.setInstanceName("newInstance"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertAllEndpointsDiscovered(discoveryEndpoints, 2);
            }
        });

        createDataIn(clusterA, "map3", 0, 1000);
        assertDataInFromEventually(clusterB, "map3", 0, 1000, clusterA);
    }

    @Test
    public void testMaxConnected() {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length));

        final WanReplicationConfig c = configA.getWanReplicationConfig(wanReplicationName);
        final WanPublisherConfig publisherConfig = c.getWanPublisherConfigs().iterator().next();
        publisherConfig.getProperties().put(WanReplicationProperties.MAX_ENDPOINTS.key(), 1);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        assertTargetEndpointSize(1);
        assertEquals(2, discoveryEndpoints.size());

        final Address connectedEndpoint = getTargetEndpoints().get(0);
        boolean connectedEndpointIsDiscovered = false;
        for (DiscoveryNode node : discoveryEndpoints) {
            connectedEndpointIsDiscovered |= node.getPublicAddress().equals(connectedEndpoint);
        }
        assertTrue(connectedEndpointIsDiscovered);
    }

    @Test
    public void connectToPrivateAddress() {
        final HashMap<String, Comparable> publisherProperties = new HashMap<String, Comparable>();
        publisherProperties.put(WanReplicationProperties.DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS.key(), true);
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(
                new ArrayListDiscoveryStrategyFactory(), publisherProperties);
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length, false));

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        assertAllEndpointsDiscovered(discoveryEndpoints, 2);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void ambiguousDiscoveryConfiguration() {
        final ArrayListDiscoveryStrategyFactory factory = setupDiscoveryStrategyFactory(new ArrayListDiscoveryStrategyFactory());
        final ArrayList<DiscoveryNode> discoveryEndpoints = factory.strategy.nodes;
        discoveryEndpoints.addAll(clusterDiscoveryNodes(configB, clusterB.length));

        final WanReplicationConfig c = configA.getWanReplicationConfig(wanReplicationName);
        final WanPublisherConfig publisherConfig = c.getWanPublisherConfigs().iterator().next();
        publisherConfig.getProperties().put(WanReplicationProperties.ENDPOINTS.key(), "192.168.0.1");

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
    }

    private <T extends DiscoveryStrategyFactory> T setupDiscoveryStrategyFactory(T discoveryStrategyFactory) {
        return setupDiscoveryStrategyFactory(discoveryStrategyFactory, Collections.<String, Comparable>emptyMap());
    }

    private <T extends DiscoveryStrategyFactory> T setupDiscoveryStrategyFactory(T discoveryStrategyFactory,
                                                                                 Map<String, Comparable> publisherProperties) {
        final DiscoveryStrategyConfig discoveryConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory);
        addDiscoveryConfig(configA, wanReplicationName, discoveryConfig);
        final WanPublisherConfig publisherConfig =
                configA.getWanReplicationConfig(wanReplicationName).getWanPublisherConfigs().iterator().next();
        publisherConfig.getProperties().put(WanReplicationProperties.DISCOVERY_PERIOD.key(), 1);
        publisherConfig.getProperties().putAll(publisherProperties);
        return discoveryStrategyFactory;
    }

    private void assertTargetEndpointSize(int expectedSize) {
        assertEquals(expectedSize, getTargetEndpoints().size());
    }

    private List<Address> getTargetEndpoints() {
        final EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(clusterA[0]);
        final WanBatchReplication endpoint = (WanBatchReplication) wanReplicationService.getEndpointOrFail(
                wanReplicationName, configB.getGroupConfig().getName());
        return endpoint.getTargetEndpoints();
    }


    private void assertAllEndpointsDiscovered(Collection<DiscoveryNode> strategyEndpoints, int expectedSize) {
        final List<Address> targetEndpoints = getTargetEndpoints();
        assertEquals(expectedSize, targetEndpoints.size());
        assertEquals(strategyEndpoints.size(), targetEndpoints.size());
        for (DiscoveryNode node : strategyEndpoints) {
            targetEndpoints.contains(node.getPublicAddress());
        }
    }

    private static ArrayList<DiscoveryNode> clusterDiscoveryNodes(Config config, int count) {
        return clusterDiscoveryNodes(config, count, true);
    }

    private static ArrayList<DiscoveryNode> clusterDiscoveryNodes(Config config, int count, boolean hasPublicAddress) {
        try {
            final ArrayList<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>();
            int port = config.getNetworkConfig().getPort();
            for (int i = 0; i < count; i++) {
                final Address addr = new Address("127.0.0.1", port++);
                nodes.add(new SimpleDiscoveryNode(addr, hasPublicAddress ? addr : null));
            }
            return nodes;
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private void addDiscoveryConfig(Config config, String setupName, DiscoveryStrategyConfig discoveryStrategyConfig) {
        final WanReplicationConfig c = config.getWanReplicationConfig(setupName);
        final WanPublisherConfig publisherConfig = c.getWanPublisherConfigs().iterator().next();
        final DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.addDiscoveryStrategyConfig(discoveryStrategyConfig);
        publisherConfig.setDiscoveryConfig(discoveryConfig);
    }

    public static class ExceptionThrowingDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        private ExceptionThrowingDiscoveryStrategy strategy =
                new ExceptionThrowingDiscoveryStrategy(null, Collections.<String, Comparable>emptyMap());

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return ExceptionThrowingDiscoveryStrategy.class;
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

    public static class ExceptionThrowingDiscoveryStrategy extends AbstractDiscoveryStrategy {

        private final ArrayList<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>();
        private boolean throwException;

        ExceptionThrowingDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            if (throwException) {
                throw new RuntimeException("BOOM");
            }
            return nodes;
        }
    }

    public static class ArrayListDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        private ArrayListDiscoveryStrategy strategy
                = new ArrayListDiscoveryStrategy(null, Collections.<String, Comparable>emptyMap());

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
