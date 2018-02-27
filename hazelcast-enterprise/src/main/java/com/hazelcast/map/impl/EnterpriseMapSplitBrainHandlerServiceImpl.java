package com.hazelcast.map.impl;

import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

class EnterpriseMapSplitBrainHandlerServiceImpl extends MapSplitBrainHandlerService {

    private final MapHDMergeHelper hdMergeHelper;

    EnterpriseMapSplitBrainHandlerServiceImpl(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.hdMergeHelper = new MapHDMergeHelper(mapServiceContext.getNodeEngine(),
                mapServiceContext.getPartitionContainers());
    }

    @Override
    protected void onPrepareMergeRunnableStart() {
        super.onPrepareMergeRunnableStart();

        hdMergeHelper.prepare();
    }

    @Override
    protected Collection<Iterator<RecordStore>> iteratorsOf(int partitionId) {
        List<Iterator<RecordStore>> iterators = new LinkedList<Iterator<RecordStore>>(super.iteratorsOf(partitionId));

        Collection<RecordStore> recordStoresOfPartition = hdMergeHelper.getHDStoresOf(partitionId);
        if (recordStoresOfPartition != null) {
            iterators.add(recordStoresOfPartition.iterator());
        }

        return iterators;
    }

    @Override
    public void destroyStores(Collection<RecordStore> stores) {
        ConcurrentMap<Integer, Collection<RecordStore>> storesByPartitionId = hdMergeHelper.groupByPartitionId(stores);
        try {
            hdMergeHelper.destroyAndRemoveHDStoresFrom(storesByPartitionId);
        } finally {
            destroyOnHeapStores(storesByPartitionId.values());
        }
    }

    private void destroyOnHeapStores(Collection<Collection<RecordStore>> onHeapStores) {
        for (Collection<RecordStore> onHeapStore : onHeapStores) {
            super.destroyStores(onHeapStore);
        }
    }
}
