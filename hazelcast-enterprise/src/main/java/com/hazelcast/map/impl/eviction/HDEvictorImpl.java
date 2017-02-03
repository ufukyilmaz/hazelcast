package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.ForcedEvictable;
import com.hazelcast.map.impl.recordstore.HDStorageSCHM;
import com.hazelcast.map.impl.recordstore.HotRestartHDStorageImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Iterator;

/**
 * An {@link Evictor} for maps which have the {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE} in-memory-format.
 * <p>
 * This evictor is sampling based, so it's independent of the size of the {@link RecordStore} (it works in constant time).
 */
public class HDEvictorImpl extends EvictorImpl {

    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int FORCED_EVICTION_PERCENTAGE = 20;
    private static final int MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT = 20;

    private final HiDensityStorageInfo storageInfo;
    private final ILogger logger;

    public HDEvictorImpl(MapEvictionPolicy mapEvictionPolicy, EvictionChecker evictionChecker,
                         IPartitionService partitionService, HiDensityStorageInfo storageInfo, NodeEngine nodeEngine) {
        super(mapEvictionPolicy, evictionChecker, partitionService);
        this.storageInfo = storageInfo;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    protected Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((HDStorageSCHM.LazyEntryViewFromRecord) selectedEntry).getRecord();
    }

    public void forceEvict(RecordStore recordStore) {
        if (recordStore.size() == 0) {
            return;
        }
        boolean backup = isBackup(recordStore);

        int removalSize = calculateRemovalSize(recordStore);
        int removedEntryCount = 0;
        Storage<Data, Record> storage = recordStore.getStorage();
        Iterator<Record> recordIterator = ((ForcedEvictable<Record>) storage).newForcedEvictionValuesIterator();

        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            Data key = record.getKey();
            if (!recordStore.isLocked(key)) {
                if (!backup) {
                    recordStore.doPostEvictionOperations(record, backup);
                }
                recordStore.evict(record.getKey(), backup);
                removedEntryCount++;
            }

            if (removedEntryCount >= removalSize) {
                break;
            }
        }

        recordStore.disposeDeferredBlocks();

        if (storageInfo.increaseForceEvictionCount() == 1) {
            logger.warning("Forced eviction invoked for the first time for IMap[name=" + recordStore.getName() + "]");
        }
        storageInfo.increaseForceEvictedEntryCount(removedEntryCount);
    }

    @Override
    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();

        if (storage instanceof HotRestartHDStorageImpl) {
            return (Iterable<EntryView>) ((HotRestartHDStorageImpl) storage).getStorageImpl().getRandomSamples(SAMPLE_COUNT);
        }

        return (Iterable<EntryView>) storage.getRandomSamples(SAMPLE_COUNT);
    }

    private static int calculateRemovalSize(RecordStore recordStore) {
        int size = recordStore.size();
        int removalSize = (int) (size * (long) FORCED_EVICTION_PERCENTAGE / ONE_HUNDRED_PERCENT);
        return Math.max(removalSize, MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT);
    }
}
