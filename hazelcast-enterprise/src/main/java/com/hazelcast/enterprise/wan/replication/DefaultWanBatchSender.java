package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.EndpointManager;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.CompletedFuture;

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
            return invokeOnWanTarget(connectionWrapper, batchReplicationEvent);
        } else {
            return new CompletedFuture<Boolean>(serializationService, false, wanExecutor);
        }
    }

    private InternalCompletableFuture<Boolean> invokeOnWanTarget(WanConnectionWrapper connectionWrapper,
                                                                 DataSerializable event) {
        EndpointManager endpointManager = connectionWrapper.getConnection().getEndpointManager();
        Address target = connectionWrapper.getConnection().getEndPoint();
        Operation wanOperation = new WanOperation(serializationService.toData(event),
                configurationContext.getAcknowledgeType());
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        return operationService.createInvocationBuilder(serviceName, wanOperation, target)
                .setTryCount(1)
                .setEndpointManager(endpointManager)
                .invoke();
    }
}
