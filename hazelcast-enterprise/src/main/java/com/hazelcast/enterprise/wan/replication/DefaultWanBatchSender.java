package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ACK_TYPE;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.RESPONSE_TIMEOUT_MILLIS;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.getProperty;

/**
 * The default implementation of the {@link WanBatchSender}.
 * It will send the WAN batch as a {@link WanOperation} to the target
 * address and block for a configurable amount of time. It will also
 * remove a connection if there was an exception while sending the
 * operation.
 */
public class DefaultWanBatchSender implements WanBatchSender {
    private static final long DEFAULT_RESPONSE_TIMEOUT_MILLIS = 60000;
    private final WanConnectionManager connectionManager;
    private final ILogger logger;
    private final SerializationService serializationService;
    private final OperationService operationService;
    private final long responseTimeoutMillis;
    private final WanAcknowledgeType acknowledgeType;

    public DefaultWanBatchSender(WanConnectionManager connectionManager,
                                 ILogger logger,
                                 SerializationService serializationService,
                                 OperationService operationService,
                                 Map<String, Comparable> wanPublisherProperties) {
        this.connectionManager = connectionManager;
        this.logger = logger;
        this.serializationService = serializationService;
        this.operationService = operationService;
        this.responseTimeoutMillis =
                getProperty(RESPONSE_TIMEOUT_MILLIS, wanPublisherProperties, DEFAULT_RESPONSE_TIMEOUT_MILLIS);
        this.acknowledgeType = WanAcknowledgeType.valueOf(
                getProperty(ACK_TYPE, wanPublisherProperties, WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.name()));
    }

    @Override
    public boolean send(BatchWanReplicationEvent batchReplicationEvent, Address target) {
        WanConnectionWrapper connectionWrapper = null;
        try {
            connectionWrapper = connectionManager.getConnection(target);
            if (connectionWrapper != null) {
                return invokeOnWanTarget(connectionWrapper.getConnection().getEndPoint(), batchReplicationEvent);
            }
        } catch (Throwable t) {
            logger.warning(t);
            if (connectionWrapper != null) {
                final Address address = connectionWrapper.getTargetAddress();
                connectionManager.removeTargetEndpoint(address,
                        "Error occurred when sending WAN events to " + address, t);
            }
        }
        return false;
    }

    private boolean invokeOnWanTarget(Address target, DataSerializable event) {
        final Operation wanOperation = new WanOperation(serializationService.toData(event), acknowledgeType);
        final String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        final InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(serviceName, wanOperation, target);
        final InternalCompletableFuture<Boolean> future = invocationBuilder.setTryCount(1)
                                                                           .setCallTimeout(responseTimeoutMillis)
                                                                           .invoke();
        return future.join();
    }
}
