package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ACK_TYPE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_MAX_DELAY_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.BATCH_SIZE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.GROUP_PASSWORD;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.RESPONSE_TIMEOUT_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;

/**
 * Abstract WAN event publisher implementation.
 */
public abstract class AbstractWanReplication
        extends AbstractWanPublisher {

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final long DEFAULT_BATCH_MAX_DELAY_MILLIS = 1000;
    private static final long DEFAULT_RESPONSE_TIMEOUT_MILLIS = 60000;
    private static final String DEFAULT_GROUP_PASS = "dev-pass";

    protected WanAcknowledgeType acknowledgeType;
    protected WanConnectionManager connectionManager;

    protected int batchSize;
    protected long batchMaxDelayMillis;
    protected long responseTimeoutMillis;
    protected List<String> endpointList;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig) {
        super.init(node, wanReplicationConfig, publisherConfig);

        Map<String, Comparable> props = publisherConfig.getProperties();

        this.batchSize = getProperty(BATCH_SIZE, props, DEFAULT_BATCH_SIZE);
        this.batchMaxDelayMillis = getProperty(BATCH_MAX_DELAY_MILLIS, props, DEFAULT_BATCH_MAX_DELAY_MILLIS);

        this.responseTimeoutMillis = getProperty(RESPONSE_TIMEOUT_MILLIS, props, DEFAULT_RESPONSE_TIMEOUT_MILLIS);
        String endpoint = getProperty(ENDPOINTS, props, "");

        endpointList = Arrays.asList(endpoint.split(","));
        if (endpointList.isEmpty()) {
            throw new InvalidConfigurationException("WanPublisherConfig must have at least one endpoint");
        }
        String groupPass = getProperty(GROUP_PASSWORD, props, DEFAULT_GROUP_PASS);
        this.connectionManager = new WanConnectionManager(node);
        this.connectionManager.init(targetGroupName, groupPass, endpointList);
        this.acknowledgeType = WanAcknowledgeType.valueOf(
                getProperty(ACK_TYPE, props, WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.name()));
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
        return future.getSafely();
    }

    @Override
    public boolean isConnected() {
        return connectionManager.getFailedAddressSet().isEmpty();
    }
}
