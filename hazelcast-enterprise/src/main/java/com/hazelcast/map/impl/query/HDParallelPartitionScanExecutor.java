package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class HDParallelPartitionScanExecutor extends ParallelPartitionScanExecutor {

    public static final int DEFAULT_QUERY_EXECUTION_TIMEOUT_MINUTES = 5;

    private OperationService operationService;

    public HDParallelPartitionScanExecutor(
            PartitionScanRunner partitionScanRunner, OperationService operationService, int timeoutInMinutes) {
        super(partitionScanRunner, null, timeoutInMinutes);
        this.operationService = operationService;
    }

    @Override
    protected Future<Collection<QueryableEntry>> runPartitionScanForPartition(String name, Predicate predicate, int partitionId) {
        Operation operation = new HDPartitionScanOperation(name, predicate);
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        return invocationBuilder.invoke();
    }

}
