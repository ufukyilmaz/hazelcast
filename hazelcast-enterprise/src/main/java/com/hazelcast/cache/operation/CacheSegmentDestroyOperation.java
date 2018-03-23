package com.hazelcast.cache.operation;

import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.spi.AbstractLocalOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

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
