package com.hazelcast.map.impl;

import com.hazelcast.internal.hidensity.impl.AbstractHDMergeHelper;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * @see AbstractHDMergeHelper
 */
class MapHDMergeHelper extends AbstractHDMergeHelper<RecordStore> {

    private final MapServiceContext mapServiceContext;

    MapHDMergeHelper(NodeEngine nodeEngine, MapServiceContext mapServiceContext) {
        super(nodeEngine);
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void collectHdStores(Map<String, RecordStore> collectedHdStores, int partitionId) {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        Collection<RecordStore> allRecordStores = partitionContainer.getAllRecordStores();
        Iterator<RecordStore> iterator = allRecordStores.iterator();
        while (iterator.hasNext()) {
            RecordStore recordStore = iterator.next();
            if (hdBacked(recordStore)) {
                collectedHdStores.put(recordStore.getName(), recordStore);
                iterator.remove();
            }
        }
    }

    public static boolean hdBacked(RecordStore recordStore) {
        return recordStore.getMapContainer().getMapConfig().getInMemoryFormat() == NATIVE;
    }

    @Override
    protected void destroyHdStore(RecordStore hdStore) {
        assertRunningOnPartitionThread();

        hdStore.destroy();
        hdStore.getMapContainer().getIndexes(hdStore.getPartitionId()).clearIndexes();
    }
}
