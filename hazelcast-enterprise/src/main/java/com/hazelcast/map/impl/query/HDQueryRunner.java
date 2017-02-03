package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;

import java.util.Collection;

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

    @Override
    protected Collection<QueryableEntry> runUsingPartitionScanSafely(String name, Predicate predicate,
                                                                     Collection<Integer> partitions, int migrationStamp) {
        // no partition-scan run in index+partition-scan query mode
        return null;
    }

}
