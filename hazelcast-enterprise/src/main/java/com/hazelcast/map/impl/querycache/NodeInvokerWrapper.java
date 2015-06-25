package com.hazelcast.map.impl.querycache;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Invocation functionality for node-side {@link QueryCacheContext}.
 *
 * @see InvokerWrapper
 */
public class NodeInvokerWrapper implements InvokerWrapper {

    private final OperationService operationService;

    public NodeInvokerWrapper(OperationService operationService) {
        this.operationService = operationService;
    }

    @Override
    public Future invokeOnPartitionOwner(Object operation, int partitionId) {
        checkNotNull(operation, "operation cannot be null");
        checkNotNegative(partitionId, "partitionId");

        Operation op = (Operation) operation;
        return operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(Object request) throws Exception {
        checkInstanceOf(OperationFactory.class, request, "request");

        OperationFactory factory = (OperationFactory) request;
        return operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, factory);
    }

    @Override
    public Future invokeOnTarget(Object operation, Address address) {
        checkNotNull(operation, "operation cannot be null");
        checkNotNull(address, "address cannot be null");

        Operation op = (Operation) operation;
        return operationService.invokeOnTarget(MapService.SERVICE_NAME, op, address);
    }

    @Override
    public Object invoke(Object operation) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeOperation(Operation operation) {
        checkNotNull(operation, "operation cannot be null");

        operationService.executeOperation(operation);
    }
}
