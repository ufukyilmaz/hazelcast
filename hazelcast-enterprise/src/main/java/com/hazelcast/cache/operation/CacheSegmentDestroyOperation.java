package com.hazelcast.cache.operation;

import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

/**
 * Destroys the segments for specified cache name.
 */
public final class CacheSegmentDestroyOperation
        extends AbstractLocalOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState {
    private String name;

    public CacheSegmentDestroyOperation() {
    }

    public CacheSegmentDestroyOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        int partitionId = getPartitionId();
        EnterpriseCacheService service = getService();
        CachePartitionSegment segment = service.getSegment(partitionId);
        segment.deleteRecordStore(name, true);
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }
}
