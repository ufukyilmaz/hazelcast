package com.hazelcast.cache.impl;

import com.hazelcast.internal.hidensity.impl.AbstractHDMergeHelper;
import com.hazelcast.spi.NodeEngine;

import java.util.Iterator;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * @see AbstractHDMergeHelper
 */
class CacheHDMergeHelper extends AbstractHDMergeHelper<ICacheRecordStore> {

    protected final CachePartitionSegment[] segments;

    CacheHDMergeHelper(NodeEngine nodeEngine, CachePartitionSegment[] segments) {
        super(nodeEngine);
        this.segments = segments;
    }

    @Override
    protected Iterator<ICacheRecordStore> storeIterator(int partitionId) {
        return segments[partitionId].recordStores.values().iterator();
    }

    @Override
    protected String extractHDStoreName(ICacheRecordStore store) {
        assert isHDStore(store);

        return store.getName();
    }

    @Override
    protected void destroyHDStore(ICacheRecordStore store) {
        assert isHDStore(store);
        assertRunningOnPartitionThread();

        store.destroy();
    }

    @Override
    protected boolean isHDStore(ICacheRecordStore store) {
        return store.getConfig().getInMemoryFormat() == NATIVE;
    }

    @Override
    protected int getPartitionId(ICacheRecordStore store) {
        return store.getPartitionId();
    }
}
