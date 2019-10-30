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
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.IPartitionService;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * An {@link Evictor} for maps which have the {@link
 * com.hazelcast.config.InMemoryFormat#NATIVE NATIVE} in-memory-format.
 * <p>
 * This evictor is sampling based, so it's independent of the
 * size of the {@link RecordStore} (it works in constant time).
 */
public class HDEvictorImpl extends EvictorImpl {

    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int FORCED_EVICTION_PERCENTAGE = 20;
    private static final int MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT = 20;

    private final HiDensityStorageInfo storageInfo;
    private final ILogger logger;

    public HDEvictorImpl(MapEvictionPolicy mapEvictionPolicy, EvictionChecker evictionChecker,
                         IPartitionService partitionService, HiDensityStorageInfo storageInfo, NodeEngine nodeEngine,
                         int batchSize) {
        super(mapEvictionPolicy, evictionChecker, partitionService, batchSize);
        this.storageInfo = storageInfo;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    protected Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((HDStorageSCHM.LazyEntryViewFromRecord) selectedEntry).getRecord();
    }

    @Override
    public void forceEvict(RecordStore recordStore) {
        if (recordStore.size() == 0) {
            return;
        }
        boolean backup = isBackup(recordStore);

        int removalSize = calculateRemovalSize(recordStore);
        Storage<Data, Record> storage = recordStore.getStorage();
        Iterator<Record> recordIterator = ((ForcedEvictable<Record>) storage).newForcedEvictionValuesIterator();

        Queue<Data> keysToRemove = new LinkedList<>();
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            Data key = record.getKey();
            if (!recordStore.isLocked(key)) {
                if (!backup) {
                    recordStore.doPostEvictionOperations(record);
                }
                keysToRemove.add(key);
            }

            if (keysToRemove.size() >= removalSize) {
                break;
            }
        }

        int removedKeyCount = removeKeys(keysToRemove, recordStore, backup);

        recordStore.disposeDeferredBlocks();

        if (storageInfo.increaseForceEvictionCount() == 1) {
            logger.warning("Forced eviction invoked for the first time for IMap[name=" + recordStore.getName() + "]");
        }
        storageInfo.increaseForceEvictedEntryCount(removedKeyCount);
    }

    private static int removeKeys(Queue<Data> keysToRemove, RecordStore recordStore, boolean backup) {
        int removedEntryCount = 0;

        while (!keysToRemove.isEmpty()) {
            Data keyToEvict = keysToRemove.poll();
            recordStore.evict(keyToEvict, backup);
            removedEntryCount++;
        }

        return removedEntryCount;
    }

    @Override
    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();

        if (storage instanceof HotRestartHDStorageImpl) {
            return ((HotRestartHDStorageImpl) storage).getStorageImpl().getRandomSamples(SAMPLE_COUNT);
        }
        return (Iterable<EntryView>) storage.getRandomSamples(SAMPLE_COUNT);
    }

    private static int calculateRemovalSize(RecordStore recordStore) {
        int size = recordStore.size();
        int removalSize = (int) (size * (long) FORCED_EVICTION_PERCENTAGE / ONE_HUNDRED_PERCENT);
        return Math.max(removalSize, MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT);
    }
}
