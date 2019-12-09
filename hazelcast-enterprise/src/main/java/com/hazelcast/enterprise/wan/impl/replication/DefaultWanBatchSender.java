package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.version.Version;

import java.util.concurrent.Executor;

/**
 * The default implementation of the {@link WanBatchSender}.
 * It will send the WAN batch as a {@link WanEventContainerOperation} to the target
 * address and block for a configurable amount of time. It will also
 * remove a connection if there was an exception while sending the
 * operation.
 */
public class DefaultWanBatchSender implements WanBatchSender {
    private WanConnectionManager connectionManager;
    private OperationService operationService;
    private WanConfigurationContext configurationContext;
    private Executor wanExecutor;

    @Override
    public void init(Node node, WanBatchPublisher publisher) {
        this.connectionManager = publisher.getConnectionManager();
        this.operationService = node.getNodeEngine().getOperationService();
        this.configurationContext = publisher.getConfigurationContext();
        this.wanExecutor = publisher.getWanExecutor();
    }

    @Override
    public InternalCompletableFuture<Boolean> send(WanEventBatch batchReplicationEvent,
                                                   Address target) {
        WanConnectionWrapper connectionWrapper = connectionManager.getConnection(target);
        if (connectionWrapper != null) {
            Version protocolVersion = connectionWrapper.getNegotiationResponse()
                    .getChosenWanProtocolVersion();
            return invokeOnWanTarget(connectionWrapper, batchReplicationEvent, protocolVersion);
        } else {
            return InternalCompletableFuture.newCompletedFuture(false, wanExecutor);
        }
    }

    private InternalCompletableFuture<Boolean> invokeOnWanTarget(WanConnectionWrapper connectionWrapper,
                                                                 IdentifiedDataSerializable event,
                                                                 Version wanProtocolVersion) {
        EndpointManager endpointManager = connectionWrapper.getConnection().getEndpointManager();
        Address target = connectionWrapper.getConnection().getEndPoint();
        Operation wanOperation = new WanEventContainerOperation(
                event, configurationContext.getAcknowledgeType(), wanProtocolVersion);
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        return operationService.createInvocationBuilder(serviceName, wanOperation, target)
                .setTryCount(1)
                .setEndpointManager(endpointManager)
                .invoke();
    }
}
