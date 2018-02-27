package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

class EnterpriseCacheSplitBrainHandlerServiceImpl extends CacheSplitBrainHandlerService {

    private final CacheHDMergeHelper hdMergeHelper;

    EnterpriseCacheSplitBrainHandlerServiceImpl(NodeEngine nodeEngine, Map<String, CacheConfig> configs,
                                                CachePartitionSegment[] segments) {
        super(nodeEngine, configs, segments);
        hdMergeHelper = new CacheHDMergeHelper(nodeEngine, segments);
    }

    @Override
    protected void onPrepareMergeRunnableStart() {
        super.onPrepareMergeRunnableStart();

        hdMergeHelper.prepare();
    }

    @Override
    protected Collection<Iterator<ICacheRecordStore>> iteratorsOf(int partitionId) {
        List<Iterator<ICacheRecordStore>> iterators = new LinkedList<Iterator<ICacheRecordStore>>(super.iteratorsOf(partitionId));

        Collection<ICacheRecordStore> recordStoresOfPartition = hdMergeHelper.getHDStoresOf(partitionId);
        if (recordStoresOfPartition != null) {
            iterators.add(recordStoresOfPartition.iterator());
        }

        return iterators;
    }

    @Override
    public void destroyStores(Collection<ICacheRecordStore> stores) {
        ConcurrentMap<Integer, Collection<ICacheRecordStore>> storesByPartitionId = hdMergeHelper.groupByPartitionId(stores);
        try {
            hdMergeHelper.destroyAndRemoveHDStoresFrom(storesByPartitionId);
        } finally {
            destroyOnHeapStores(storesByPartitionId.values());
        }
    }

    private void destroyOnHeapStores(Collection<Collection<ICacheRecordStore>> onHeapStores) {
        for (Collection<ICacheRecordStore> onHeapStore : onHeapStores) {
            super.destroyStores(onHeapStore);
        }
    }
}
