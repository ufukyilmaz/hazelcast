package com.hazelcast.wan.discovery;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests if misconfigured WAN does not consume resources (discovery SPI call,
 * scheduling a task) before throwing an exception because of invalid
 * configuration.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanMisconfigurationTest extends HazelcastTestSupport {
    private String wanReplicationName;
    private CountingDiscoveryStrategyFactory discoveryStrategyFactory;
    private HazelcastInstance instance;

    @Override
    protected Config getConfig() {
        final DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(discoveryStrategyFactory));
        final WanPublisherConfig wanPublisherConfig = new WanPublisherConfig()
                .setDiscoveryConfig(discoveryConfig)
                .setClassName(WanBatchReplication.class.getName());
        wanPublisherConfig.getProperties().put(WanReplicationProperties.MAX_ENDPOINTS.key(), "");
        wanPublisherConfig.getProperties().put(WanReplicationProperties.GROUP_PASSWORD.key(), "");
        final WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName(wanReplicationName)
                .addWanPublisherConfig(wanPublisherConfig);
        final WanReplicationRef wanReplicationRef = new WanReplicationRef()
                .setName(wanReplicationName)
                .setMergePolicy(PassThroughMergePolicy.class.getName());
        final MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setWanReplicationRef(wanReplicationRef);
        return super.getConfig()
                    .addMapConfig(mapConfig)
                    .addWanReplicationConfig(wanConfig);
    }

    @Before
    public void init() {
        this.wanReplicationName = "atob";
        this.discoveryStrategyFactory = new CountingDiscoveryStrategyFactory();
        this.instance = createHazelcastInstance();
    }

    @Test
    public void configurationErrorsMustBeDetectedBeforeUsingResources() {
        try {
            instance.getMap("map").put("1", "1");
        } catch (Exception e) {
            ignore(e);
        }
        final ManagedExecutorService wanPollerExecutor =
                getNode(instance).nodeEngine.getExecutionService().getExecutor("hz:wan:poller");

        assertEquals(0, wanPollerExecutor.getQueueSize());
        assertEquals(0, discoveryStrategyFactory.strategy.invocationCount);
    }

    public static class CountingDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        private CountingDiscoveryStrategy strategy = new CountingDiscoveryStrategy(null, Collections.<String, Comparable>emptyMap());

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return CountingDiscoveryStrategy.class;
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

    public static class CountingDiscoveryStrategy extends AbstractDiscoveryStrategy {
        private int invocationCount;

        CountingDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            invocationCount++;
            return Collections.emptyList();
        }
    }
}
