package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class EnterpriseCacheSplitBrainHandlerServiceImpl extends CacheSplitBrainHandlerService {

    private final CacheHDMergeHelper hdMergeHelper;

    EnterpriseCacheSplitBrainHandlerServiceImpl(NodeEngine nodeEngine, Map<String, CacheConfig> configs,
                                                CachePartitionSegment[] segments) {
        super(nodeEngine, configs, segments);
        hdMergeHelper = new CacheHDMergeHelper(nodeEngine, segments);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        hdMergeHelper.prepare();
        return super.prepareMergeRunnable();
    }

    @Override
    protected List<Iterator<ICacheRecordStore>> iteratorsOf(CachePartitionSegment segment) {
        List<Iterator<ICacheRecordStore>> iterators = new LinkedList<Iterator<ICacheRecordStore>>(super.iteratorsOf(segment));

        Collection<ICacheRecordStore> recordStoresOfPartition = hdMergeHelper.getStoresOf(segment.partitionId);
        if (recordStoresOfPartition != null) {
            iterators.add(recordStoresOfPartition.iterator());
        }

        return iterators;
    }

    @Override
    protected void destroySegment(CachePartitionSegment segment) {
        try {
            hdMergeHelper.destroyCollectedHdStores();
        } finally {
            super.destroySegment(segment);
        }
    }
}
