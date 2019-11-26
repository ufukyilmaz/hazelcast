package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.ForcedEvictable;
import com.hazelcast.map.impl.recordstore.HDStorageSCHM;
import com.hazelcast.map.impl.recordstore.HotRestartHDStorageImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import java.util.Iterator;
import java.util.Map;

/**
 * An {@link Evictor} for maps which have the {@link
 * com.hazelcast.config.InMemoryFormat#NATIVE NATIVE} in-memory-format.
 * <p>
 * This evictor is sampling based, so it's independent of the
 * size of the {@link RecordStore} (it works in constant time).
 */
public class HDEvictorImpl extends EvictorImpl {

    private static final int MAX_PER_PASS_REMOVAL_COUNT = 1024;
    private static final int MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT = 20;

    private final HiDensityStorageInfo storageInfo;
    private final ILogger logger;

    public HDEvictorImpl(EvictionPolicyComparator evictionPolicyComparator,
                         EvictionChecker evictionChecker, int batchSize,
                         IPartitionService partitionService,
                         HiDensityStorageInfo storageInfo,
                         ILogger logger) {
        super(evictionPolicyComparator, evictionChecker, batchSize, partitionService);
        this.storageInfo = storageInfo;
        this.logger = logger;
    }

    @Override
    protected Record getRecordFromEntryView(EntryView evictableEntryView) {
        return ((HDStorageSCHM.LazyEvictableEntryView) evictableEntryView).getRecord();
    }

    @Override
    protected Data getDataKeyFromEntryView(EntryView evictableEntryView) {
        return ((HDStorageSCHM.LazyEvictableEntryView) evictableEntryView).getDataKey();
    }

    @Override
    public void forceEvictByPercentage(RecordStore recordStore, double percentage) {
        assert percentage > 0 && percentage <= 1
                : "wrong percentage found " + percentage;

        if (recordStore.size() == 0) {
            return;
        }
        boolean backup = isBackup(recordStore);

        int removeThisNumOfEntries = (int) Math.max(recordStore.size() * percentage,
                MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT);
        int maxRemovePerPass = Math.min(removeThisNumOfEntries, MAX_PER_PASS_REMOVAL_COUNT);
        Data[] keysToRemove = new Data[maxRemovePerPass];
        do {
            evictRecordStore(recordStore, maxRemovePerPass, backup, keysToRemove);
            removeThisNumOfEntries -= maxRemovePerPass;
        } while (recordStore.size() > 0 && removeThisNumOfEntries > 0);
    }

    private void evictRecordStore(RecordStore recordStore, int maxRemovePerPass,
                                  boolean backup, Data[] keysToRemove) {
        int index = -1;
        Iterator<Map.Entry<Data, Record>> recordIterator = getEntryIterator(recordStore, maxRemovePerPass);
        while (recordIterator.hasNext()) {
            Map.Entry<Data, Record> entry = recordIterator.next();
            Data key = entry.getKey();

            if (!recordStore.isLocked(key)) {
                if (!backup) {
                    recordStore.doPostEvictionOperations(key, entry.getValue());
                }
                keysToRemove[++index] = key;
            }

            if ((index + 1) == maxRemovePerPass) {
                break;
            }
        }

        int removedKeyCount = removeKeys(keysToRemove, recordStore, backup);

        recordStore.disposeDeferredBlocks();

        if (storageInfo.increaseForceEvictionCount() == 1) {
            logger.warning("Forced eviction invoked for the first"
                    + " time for IMap[name=" + recordStore.getName() + "]");
        }
        storageInfo.increaseForceEvictedEntryCount(removedKeyCount);
    }

    private Iterator<Map.Entry<Data, Record>> getEntryIterator(RecordStore recordStore, int maxRemovePerPass) {
        Storage storage = recordStore.getStorage();
        return maxRemovePerPass == recordStore.size()
                ? storage.mutationTolerantIterator()
                : ((ForcedEvictable<Data, Record>) storage).newRandomEvictionEntryIterator();
    }

    private static int removeKeys(Data[] keysToRemove,
                                  RecordStore recordStore, boolean backup) {
        int removedEntryCount = 0;

        for (int i = 0; i < keysToRemove.length; i++) {
            Data key = keysToRemove[i];
            keysToRemove[i] = null;
            if (key == null) {
                break;
            }

            recordStore.evict(key, backup);
            removedEntryCount++;
        }

        return removedEntryCount;
    }

    @Override
    protected Iterable<EntryView> getRandomSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();

        if (storage instanceof HotRestartHDStorageImpl) {
            return ((HotRestartHDStorageImpl) storage).getStorageImpl().getRandomSamples(SAMPLE_COUNT);
        }
        return (Iterable<EntryView>) storage.getRandomSamples(SAMPLE_COUNT);
    }
}
