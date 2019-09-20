package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.impl.discovery.StaticDiscoveryProperties;
import com.hazelcast.enterprise.wan.impl.discovery.StaticDiscoveryStrategy;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.impl.PredefinedDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Abstract WAN event publisher implementation.
 */
public abstract class AbstractWanReplication extends AbstractWanPublisher {

    WanConnectionManager connectionManager;

    private DiscoveryService discoveryService;

    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig publisherConfig) {
        WanBatchReplicationPublisherConfig batchReplicationConfig = (WanBatchReplicationPublisherConfig) publisherConfig;
        super.init(wanReplicationConfig, publisherConfig);

        this.discoveryService = checkNotNull(createDiscoveryService(batchReplicationConfig));
        this.discoveryService.start();

        this.connectionManager = new WanConnectionManager(node, discoveryService);
        this.connectionManager.init(configurationContext);
    }

    private DiscoveryService createDiscoveryService(WanBatchReplicationPublisherConfig config) {
        final String endpoints = configurationContext.getEndpoints();
        final DiscoveryConfig discoveryConfig = config.getDiscoveryConfig();
        final boolean endpointsConfigured = !isNullOrEmpty(endpoints);
        final boolean discoveryEnabled = (discoveryConfig != null && discoveryConfig.isEnabled())
                || !AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config).isEmpty();

        if (endpointsConfigured) {
            if (discoveryEnabled) {
                throw ambiguousPublisherConfig();
            }
            return new PredefinedDiscoveryService(staticDiscoveryStrategy(endpoints));
        }
        if (discoveryEnabled) {
            return node.createDiscoveryService(config.getDiscoveryConfig(),
                    AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config),
                    node.getLocalMember());
        }
        throw new InvalidConfigurationException("There are no methods of defining publisher endpoints. "
                + "Either use the the discovery configuration or define static endpoints");
    }

    private static InvalidConfigurationException ambiguousPublisherConfig() {
        return new InvalidConfigurationException("The publisher endpoint configuration is ambiguous. "
                + "Either use the the discovery configuration or define static endpoints");
    }

    private StaticDiscoveryStrategy staticDiscoveryStrategy(String endpoints) {
        final Map<String, Comparable> properties = new HashMap<>();
        properties.put(StaticDiscoveryProperties.ENDPOINTS.key(), endpoints);
        properties.put(StaticDiscoveryProperties.PORT.key(), node.getConfig().getNetworkConfig().getPort());
        return new StaticDiscoveryStrategy(logger, properties);
    }

    @Override
    public boolean isConnected() {
        return connectionManager.isConnected();
    }

    /**
     * Return a snapshot of the list of currently known target endpoints to which replication is made. Some of them can
     * currently have dead connections and are about to be removed.
     *
     * @return the list of currently known target endpoints
     */
    public List<Address> getTargetEndpoints() {
        return connectionManager.getTargetEndpoints();
    }

    // public for testing
    public WanConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    protected void afterShutdown() {
        super.afterShutdown();
        connectionManager.shutdown();
        if (discoveryService != null) {
            try {
                discoveryService.destroy();
            } catch (Exception e) {
                logger.warning("Could not destroy discovery service", e);
            }
        }
    }
}
