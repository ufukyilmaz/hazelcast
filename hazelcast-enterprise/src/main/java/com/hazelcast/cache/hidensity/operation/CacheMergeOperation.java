package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

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
    private List<MergingEntry<Data, Data>> mergingEntries;

    private transient CacheWanEventPublisher wanEventPublisher;

    private transient boolean hasBackups;
    private transient boolean wanReplicationEnabled;
    private transient Map<Data, CacheRecord> backupRecords;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, List<MergingEntry<Data, Data>> mergingEntries, SplitBrainMergePolicy policy) {
        super(name);
        this.mergingEntries = mergingEntries;
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
            backupRecords = createHashMap(mergingEntries.size());
        }
    }

    @Override
    public void runInternal() {
        for (MergingEntry<Data, Data> mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }

        response = true;
    }

    private void merge(MergingEntry<Data, Data> mergingEntry) {
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
        out.writeInt(mergingEntries.size());
        for (MergingEntry<Data, Data> mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergingEntries = new ArrayList<MergingEntry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            MergingEntry<Data, Data> mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.MERGE;
    }
}
