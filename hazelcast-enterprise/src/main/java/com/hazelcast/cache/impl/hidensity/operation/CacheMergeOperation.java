package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.spi.impl.merge.CacheMergingEntryImpl;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.impl.CallerProvenance;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends BackupAwareHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy;
    private List<CacheMergeTypes<Object, Object>> mergingEntries;

    private transient boolean hasBackups;
    private transient boolean wanReplicationEnabled;
    private transient Map<Data, CacheRecord> backupRecords;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, List<CacheMergeTypes<Object, Object>> mergingEntries,
                               SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> policy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = policy;
    }

    @Override
    public void beforeRunInternal() {
        wanReplicationEnabled = recordStore.isWanReplicationEnabled();

        hasBackups = getSyncBackupCount() + getAsyncBackupCount() > 0;
        if (hasBackups) {
            backupRecords = createHashMap(mergingEntries.size());
        }
    }

    @Override
    public void runInternal() {
        for (CacheMergeTypes mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }
        response = true;
    }

    private void merge(CacheMergeTypes mergingEntry) {
        assert mergingEntry instanceof CacheMergingEntryImpl;
        CacheMergingEntryImpl mergingEntryImpl = (CacheMergingEntryImpl) mergingEntry;
        CacheRecord backupRecord = recordStore.merge(mergingEntry, mergePolicy, CallerProvenance.NOT_WAN);

        if (hasBackups && backupRecord != null) {
            backupRecords.put(mergingEntryImpl.getRawKey(), backupRecord);
        }

        if (wanReplicationEnabled) {
            if (backupRecord != null) {
                publishWanUpdate(mergingEntryImpl.getRawKey(), backupRecord);
            } else {
                publishWanRemove(mergingEntryImpl.getRawKey());
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
        for (CacheMergeTypes mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergingEntries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            CacheMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.MERGE;
    }
}
