package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends BackupAwareHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private SplitBrainMergePolicy mergePolicy;
    private List<SplitBrainMergeEntryView<Data, Data>> mergeEntries;

    private transient CacheWanEventPublisher wanEventPublisher;

    private transient boolean hasBackups;
    private transient boolean wanReplicationEnabled;
    private transient Map<Data, CacheRecord> backupRecords;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name,
                               List<SplitBrainMergeEntryView<Data, Data>> mergeEntries,
                               SplitBrainMergePolicy policy) {
        super(name);
        this.mergeEntries = mergeEntries;
        this.mergePolicy = policy;
    }

    @Override
    public void beforeRunInternal() {
        wanReplicationEnabled = cache.isWanReplicationEnabled();
        if (wanReplicationEnabled) {
            wanEventPublisher = cacheService.getCacheWanEventPublisher();
        }

        hasBackups = getSyncBackupCount() + getAsyncBackupCount() > 0;
        if (hasBackups) {
            backupRecords = createHashMap(mergeEntries.size());
        }
    }

    @Override
    public void runInternal() {
        for (SplitBrainMergeEntryView<Data, Data> mergingEntry : mergeEntries) {
            merge(mergingEntry);
        }

        response = true;
    }

    private void merge(SplitBrainMergeEntryView<Data, Data> mergingEntry) {
        CacheRecord backupRecord = cache.merge(mergingEntry, mergePolicy);

        if (hasBackups && backupRecord != null) {
            backupRecords.put(mergingEntry.getKey(), backupRecord);
        }

        if (wanReplicationEnabled) {
            if (backupRecord != null) {
                Data dataKey = mergingEntry.getKey();
                Data dataValue = (Data) backupRecord.getValue();
                wanEventPublisher.publishWanReplicationUpdate(name, createDefaultEntryView(dataKey, dataValue, backupRecord));
            } else {
                wanEventPublisher.publishWanReplicationRemove(name, mergingEntry.getKey());
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return hasBackups && !backupRecords.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheMergeBackupOperation(name, backupRecords);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergeEntries.size());
        for (SplitBrainMergeEntryView<Data, Data> mergeEntry : mergeEntries) {
            out.writeObject(mergeEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergeEntries = new ArrayList<SplitBrainMergeEntryView<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            SplitBrainMergeEntryView<Data, Data> mergeEntry = in.readObject();
            mergeEntries.add(mergeEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.MERGE;
    }
}
