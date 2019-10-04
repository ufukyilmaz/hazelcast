package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.impl.operation.WanOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.util.executor.CompletedFuture;
import com.hazelcast.version.Version;

import java.util.concurrent.Executor;

/**
 * The default implementation of the {@link WanBatchSender}.
 * It will send the WAN batch as a {@link WanOperation} to the target
 * address and block for a configurable amount of time. It will also
 * remove a connection if there was an exception while sending the
 * operation.
 */
public class DefaultWanBatchSender implements WanBatchSender {
    private WanConnectionManager connectionManager;
    private SerializationService serializationService;
    private OperationService operationService;
    private WanConfigurationContext configurationContext;
    private Executor wanExecutor;

    @Override
    public void init(Node node, WanBatchReplication publisher) {
        this.connectionManager = publisher.getConnectionManager();
        this.serializationService = node.getNodeEngine().getSerializationService();
        this.operationService = node.getNodeEngine().getOperationService();
        this.configurationContext = publisher.getConfigurationContext();
        this.wanExecutor = publisher.getWanExecutor();
    }

    @Override
    public ICompletableFuture<Boolean> send(BatchWanReplicationEvent batchReplicationEvent,
                                            Address target) {
        WanConnectionWrapper connectionWrapper = connectionManager.getConnection(target);
        if (connectionWrapper != null) {
            Version protocolVersion = connectionWrapper.getNegotiationResponse()
                                                       .getChosenWanProtocolVersion();
            return invokeOnWanTarget(connectionWrapper, batchReplicationEvent, protocolVersion);
        } else {
            return new CompletedFuture<>(serializationService, false, wanExecutor);
        }
    }

    private InternalCompletableFuture<Boolean> invokeOnWanTarget(WanConnectionWrapper connectionWrapper,
                                                                 IdentifiedDataSerializable event,
                                                                 Version wanProtocolVersion) {
        EndpointManager endpointManager = connectionWrapper.getConnection().getEndpointManager();
        Address target = connectionWrapper.getConnection().getEndPoint();
        Operation wanOperation = new WanOperation(event, configurationContext.getAcknowledgeType(), wanProtocolVersion);
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        return operationService.createInvocationBuilder(serviceName, wanOperation, target)
                .setTryCount(1)
                .setEndpointManager(endpointManager)
                .invoke();
    }
}
