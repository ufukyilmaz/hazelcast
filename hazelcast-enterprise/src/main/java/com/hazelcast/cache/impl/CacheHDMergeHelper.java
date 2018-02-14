package com.hazelcast.cache.impl;

import com.hazelcast.internal.hidensity.impl.AbstractHDMergeHelper;
import com.hazelcast.spi.NodeEngine;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * @see AbstractHDMergeHelper
 */
class CacheHDMergeHelper extends AbstractHDMergeHelper<ICacheRecordStore> {

    protected final CachePartitionSegment[] segments;

    CacheHDMergeHelper(NodeEngine nodeEngine,
                       CachePartitionSegment[] segments) {
        super(nodeEngine);
        this.segments = segments;
    }

    @Override
    public void collectHdStores(Map<String, ICacheRecordStore> collectedHdStores, int partitionId) {
        ConcurrentMap<String, ICacheRecordStore> segmentsRecordStores = segments[partitionId].recordStores;
        Iterator<ICacheRecordStore> iterator = segmentsRecordStores.values().iterator();
        while (iterator.hasNext()) {
            ICacheRecordStore recordStore = iterator.next();
            if (hdBacked(recordStore)) {
                collectedHdStores.put(recordStore.getName(), recordStore);
                iterator.remove();
            }
        }
    }

    public static boolean hdBacked(ICacheRecordStore recordStore) {
        return recordStore.getConfig().getInMemoryFormat() == NATIVE;
    }

    @Override
    protected void destroyStore(ICacheRecordStore hdStore) {
        assertRunningOnPartitionThread();

        hdStore.destroy();
    }
}
