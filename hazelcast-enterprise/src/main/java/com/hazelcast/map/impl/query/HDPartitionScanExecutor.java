package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Collection;

public class HDPartitionScanExecutor implements PartitionScanExecutor {

    private final HDPartitionScanRunner partitionScanRunner;

    public HDPartitionScanExecutor(HDPartitionScanRunner partitionScanRunner) {
        this.partitionScanRunner = partitionScanRunner;
    }

    @Override
    public Collection<QueryableEntry> execute(String mapName, Predicate predicate, Collection<Integer> partitions) {
        if (partitions.size() != 1) {
            throw new IllegalArgumentException("HD partition scan has to be run for a single partition");
        }
        int partitionId = partitions.iterator().next();
        return partitionScanRunner.run(mapName, predicate, partitionId);
    }

}
