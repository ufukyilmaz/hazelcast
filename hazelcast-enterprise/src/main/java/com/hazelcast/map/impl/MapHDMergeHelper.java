package com.hazelcast.map.impl;

import com.hazelcast.internal.hidensity.impl.AbstractHDMergeHelper;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.NodeEngine;

import java.util.Iterator;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * @see AbstractHDMergeHelper
 */
class MapHDMergeHelper extends AbstractHDMergeHelper<RecordStore> {

    private final PartitionContainer[] partitionContainers;

    MapHDMergeHelper(NodeEngine nodeEngine, PartitionContainer[] partitionContainers) {
        super(nodeEngine);
        this.partitionContainers = partitionContainers;
    }

    @Override
    protected Iterator<RecordStore> storeIterator(int partitionId) {
        return partitionContainers[partitionId].getAllRecordStores().iterator();
    }

    @Override
    protected String extractHDStoreName(RecordStore store) {
        assert isHDStore(store);

        return store.getName();
    }

    @Override
    protected void destroyHDStore(RecordStore store) {
        assert isHDStore(store);
        assertRunningOnPartitionThread();

        store.destroy();
        store.getMapContainer().getIndexes(store.getPartitionId()).clearIndexes();
    }

    @Override
    protected boolean isHDStore(RecordStore store) {
        return store.getMapContainer().getMapConfig().getInMemoryFormat() == NATIVE;
    }
}
