package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Runs query operations in the calling thread (thus blocking it)
 * <p>
 * Used by query operations only: QueryOperation & QueryPartitionOperation
 * Should not be used by proxies or any other query related objects.
 */
public class HDQueryRunner extends QueryRunner {

    public HDQueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer,
                         PartitionScanExecutor partitionScanExecutor, ResultProcessorRegistry resultProcessorRegistry) {
        super(mapServiceContext, optimizer, partitionScanExecutor, resultProcessorRegistry);
    }

    // full query = index query (if possible), then partition-scan query
    public Result runIndexOrPartitionScanQueryOnOwnedPartitions(Query query)
            throws ExecutionException, InterruptedException {
        int migrationStamp = getMigrationStamp();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();
        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());

        // first we optimize the query
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), mapContainer.getIndexes());

        // then we try to run using an index, but if that doesn't work, we'll try a full table scan
        // This would be the point where a query-plan should be added. It should determine f a full table scan
        // or an index should be used.
        Collection<QueryableEntry> entries = runUsingIndexSafely(predicate, mapContainer, migrationStamp);
        updateStatistics(mapContainer);
        if (entries != null) {
            // if results have been returned and partition state has not changed, set the partition IDs
            // so that caller is aware of partitions from which results were obtained.
            return populateTheResult(query, entries, initialPartitions);
        }

        // run partition scans on partition threads
        return runUsingPartitionScanOnPartitionThreads(query, initialPartitions);
    }

    private Result runUsingPartitionScanOnPartitionThreads(Query query, Collection<Integer> partitionIds) {
        // dispatching query-operation for all local partition
        List<Future<Result>> futures = new ArrayList<Future<Result>>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            Operation op = new QueryPartitionOperation(query);
            op.setPartitionId(partitionId);
            try {
                futures.add(operationService.<Result>invokeOnPartition(MapService.SERVICE_NAME, op, partitionId));
            } catch (Exception e) {
                logger.severe("Error while invoking QueryPartitionOperation for partitionId=" + partitionId, e);
            }
        }

        // gathering result
        Result result = resultProcessorRegistry.get(query.getResultType()).populateResult(query,
                queryResultSizeLimiter.getNodeResultLimit(partitionIds.size()));
        for (Future<Result> future : futures) {
            Result partitionResult = getResult(future);
            if (partitionResult != null) {
                result.combine(partitionResult);
            }
        }
        return result;
    }

    private Result<?> getResult(Future<Result> future) {
        try {
            return future.get();
        } catch (Exception e) {
            logger.severe("Error while getting result of QueryPartitionOperation", e);
        }
        return null;
    }

}
