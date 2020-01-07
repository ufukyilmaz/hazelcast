package com.hazelcast.map.impl.query;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntriesSegment;

import java.util.Collection;

public class HDPartitionScanExecutor implements PartitionScanExecutor {

    private final HDPartitionScanRunner partitionScanRunner;

    public HDPartitionScanExecutor(HDPartitionScanRunner partitionScanRunner) {
        this.partitionScanRunner = partitionScanRunner;
    }

    @Override
    public void execute(String mapName, Predicate predicate, Collection<Integer> partitions, Result result) {
        if (partitions.size() != 1) {
            throw new IllegalArgumentException("HD partition scan has to be run for a single partition");
        }
        int partitionId = partitions.iterator().next();
        partitionScanRunner.run(mapName, predicate, partitionId, result);
    }

    @Override
    public QueryableEntriesSegment execute(
            String mapName, Predicate predicate, int partitionId,
            IterationPointer[] pointers, int fetchSize) {
        return partitionScanRunner.run(mapName, predicate, partitionId, pointers, fetchSize);
    }

}
