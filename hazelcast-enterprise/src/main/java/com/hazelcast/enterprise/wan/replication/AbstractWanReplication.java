package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.discovery.StaticDiscoveryProperties;
import com.hazelcast.enterprise.wan.discovery.StaticDiscoveryStrategy;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.PredefinedDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ACK_TYPE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_MAX_DELAY_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_SIZE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.RESPONSE_TIMEOUT_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * Abstract WAN event publisher implementation.
 */
public abstract class AbstractWanReplication extends AbstractWanPublisher {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final long DEFAULT_BATCH_MAX_DELAY_MILLIS = 1000;
    private static final long DEFAULT_RESPONSE_TIMEOUT_MILLIS = 60000;

    protected WanAcknowledgeType acknowledgeType;
    protected WanConnectionManager connectionManager;

    protected int batchSize;
    protected long batchMaxDelayMillis;
    protected long responseTimeoutMillis;
    private DiscoveryService discoveryService;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        super.init(node, wanReplicationConfig, publisherConfig);
        final Map<String, Comparable> publisherProps = publisherConfig.getProperties();

        this.batchSize = getProperty(BATCH_SIZE, publisherProps, DEFAULT_BATCH_SIZE);
        this.batchMaxDelayMillis = getProperty(BATCH_MAX_DELAY_MILLIS, publisherProps, DEFAULT_BATCH_MAX_DELAY_MILLIS);
        this.responseTimeoutMillis = getProperty(RESPONSE_TIMEOUT_MILLIS, publisherProps, DEFAULT_RESPONSE_TIMEOUT_MILLIS);
        this.acknowledgeType = WanAcknowledgeType.valueOf(
                getProperty(ACK_TYPE, publisherProps, WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.name()));

        this.discoveryService = checkNotNull(createDiscoveryService(publisherConfig));
        this.discoveryService.start();

        this.connectionManager = new WanConnectionManager(node, discoveryService);
        this.connectionManager.init(targetGroupName, publisherProps);
    }

    private DiscoveryService createDiscoveryService(WanPublisherConfig config) {
        final String endpoints = getProperty(ENDPOINTS, config.getProperties(), "");
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

    protected boolean invokeOnWanTarget(Address target, DataSerializable event) {
        Operation wanOperation
                = new WanOperation(node.nodeEngine.getSerializationService().toData(event), acknowledgeType);
        OperationService operationService = node.nodeEngine.getOperationService();
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        InvocationBuilder invocationBuilder
                = operationService.createInvocationBuilder(serviceName, wanOperation, target);
        InternalCompletableFuture<Boolean> future = invocationBuilder.setTryCount(1)
                .setCallTimeout(responseTimeoutMillis)
                .invoke();
        return future.join();
    }

    /**
     * Return {@code true} if this publisher is aware of at least one target endpoint. This means that the endpoint
     * was discovered at some point (initially or afterwards) but doesn't necessarily mean that WAN has ever connected
     * to that endpoint or if that endpoint is still <i>live</i>. If the endpoint is dead, this will be discovered on
     * the next event sent to that endpoint at which point the endpoint is removed from the discovered endpoint list.
     * <p>
     * NOTE : Previously this method returned if the failed endpoint list was empty. This could mean that this publisher
     * hasn't yet connected to an endpoint or that all endpoints are considered <i>live</i>
     *
     * @return if this publisher is aware of at least one endpoint (has discovered it)
     */
    @Override
    public boolean isConnected() {
        return getTargetEndpoints().size() > 0;
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

    @Override
    protected void afterShutdown() {
        super.afterShutdown();
        if (discoveryService != null) {
            try {
                discoveryService.destroy();
            } catch (Exception e) {
                logger.warning("Could not destroy discovery service", e);
            }
        }
    }
}
