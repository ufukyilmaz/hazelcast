package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

class EnterpriseMapSplitBrainHandlerServiceImpl extends MapSplitBrainHandlerService {

    private final MapHDMergeHelper hdMergeHelper;

    EnterpriseMapSplitBrainHandlerServiceImpl(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.hdMergeHelper = new MapHDMergeHelper(nodeEngine, mapServiceContext.getPartitionContainers());
    }

    @Override
    public Runnable prepareMergeRunnable() {
        hdMergeHelper.prepare();
        return super.prepareMergeRunnable();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RecordStore getOrNullRecordStore(String mapName, InMemoryFormat inMemoryFormat, int partitionId) {
        if (inMemoryFormat == NATIVE) {
            return hdMergeHelper.getOrNullHDStore(mapName, partitionId);
        }

        return super.getOrNullRecordStore(mapName, inMemoryFormat, partitionId);
    }

    @Override
    protected void destroyRecordStores(Collection<RecordStore> recordStores) {
        try {
            hdMergeHelper.destroyCollectedHDStores();
        } finally {
            super.destroyRecordStores(recordStores);
        }
    }
}
