package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.discovery.StaticDiscoveryProperties;
import com.hazelcast.enterprise.wan.discovery.StaticDiscoveryStrategy;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.PredefinedDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * Abstract WAN event publisher implementation.
 */
public abstract class AbstractWanReplication extends AbstractWanPublisher {

    protected WanConnectionManager connectionManager;

    private DiscoveryService discoveryService;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        super.init(node, wanReplicationConfig, publisherConfig);

        this.discoveryService = checkNotNull(createDiscoveryService(publisherConfig));
        this.discoveryService.start();

        this.connectionManager = new WanConnectionManager(node, discoveryService);
        this.connectionManager.init(configurationContext);
    }

    private DiscoveryService createDiscoveryService(WanPublisherConfig config) {
        final String endpoints = configurationContext.getEndpoints();
        final AwsConfig awsConfig = config.getAwsConfig();
        final DiscoveryConfig discoveryConfig = config.getDiscoveryConfig();
        final boolean endpointsConfigured = !isNullOrEmpty(endpoints);
        final boolean awsEnabled = awsConfig != null && awsConfig.isEnabled();
        final boolean discoveryEnabled = discoveryConfig != null && discoveryConfig.isEnabled();

        if (endpointsConfigured) {
            if (awsEnabled || discoveryEnabled) {
                throw ambiguousPublisherConfig();
            }
            return new PredefinedDiscoveryService(staticDiscoveryStrategy(endpoints));
        }
        if (awsEnabled) {
            if (discoveryEnabled) {
                throw ambiguousPublisherConfig();
            }
            return new PredefinedDiscoveryService(awsDiscoveryStrategy(awsConfig));
        }
        if (discoveryEnabled) {
            return node.createDiscoveryService(config.getDiscoveryConfig(), node.getLocalMember());
        }
        throw new InvalidConfigurationException("There are no methods of defining publisher endpoints. "
                + "Either use the AWS configuration, the discovery configuration or define static endpoints");
    }

    private static InvalidConfigurationException ambiguousPublisherConfig() {
        return new InvalidConfigurationException("The publisher endpoint configuration is ambiguous. "
                + "Either use the AWS configuration, the discovery configuration or define static endpoints");
    }

    private DiscoveryStrategy awsDiscoveryStrategy(AwsConfig awsConfig) {
        try {
            final Class<?> clazz = Class.forName("com.hazelcast.aws.AwsDiscoveryStrategy");
            final Constructor constructor = clazz.getConstructor(Map.class);
            final Map<String, Comparable> props = new HashMap<String, Comparable>();
            props.put("access-key", awsConfig.getAccessKey());
            props.put("secret-key", awsConfig.getSecretKey());
            props.put("region", awsConfig.getRegion());
            props.put("iam-role", awsConfig.getIamRole());
            props.put("host-header", awsConfig.getHostHeader());
            props.put("security-group-name", awsConfig.getSecurityGroupName());
            props.put("tag-key", awsConfig.getTagKey());
            props.put("tag-value", awsConfig.getTagValue());
            props.put("connection-timeout-seconds", awsConfig.getConnectionTimeoutSeconds());
            props.put("hz-port", node.getConfig().getNetworkConfig().getPort());
            return (DiscoveryStrategy) constructor.newInstance(props);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private StaticDiscoveryStrategy staticDiscoveryStrategy(String endpoints) {
        final Map<String, Comparable> properties = new HashMap<String, Comparable>();
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

    WanConnectionManager getConnectionManager() {
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
