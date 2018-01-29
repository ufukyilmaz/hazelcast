package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.MERGED;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Contains multiple merge entries for split-brain healing with with a {@link SplitBrainMergePolicy}.
 */
public class HDMergeOperation extends HDMapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private List<SplitBrainMergeEntryView<Data, Data>> mergeEntries;
    private SplitBrainMergePolicy mergePolicy;

    private transient int size;
    private transient int currentIndex;

    private transient boolean hasMapListener;
    private transient boolean hasWanReplication;
    private transient boolean hasBackups;
    private transient boolean hasInvalidation;

    private transient MapEntries mapEntries;
    private transient List<RecordInfo> backupRecordInfos;
    private transient List<Data> invalidationKeys;
    private transient boolean hasMergedValues;

    public HDMergeOperation() {
    }

    HDMergeOperation(String name, List<SplitBrainMergeEntryView<Data, Data>> mergeEntries, SplitBrainMergePolicy policy) {
        super(name);
        this.mergeEntries = mergeEntries;
        this.mergePolicy = policy;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        size = mergeEntries.size();

        hasMapListener = mapEventPublisher.hasEventListener(name);
        hasWanReplication = mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null;
        hasBackups = mapContainer.getTotalBackupCount() > 0;
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            mapEntries = new MapEntries(size);
            backupRecordInfos = new ArrayList<RecordInfo>(size);
        }
        if (hasInvalidation) {
            invalidationKeys = new ArrayList<Data>(size);
        }
    }

    @Override
    public void runInternal() {
        // if currentIndex is not zero, this is a continuation of the operation after a NativeOOME
        while (currentIndex < size) {
            merge(mergeEntries.get(currentIndex));
            currentIndex++;
        }
    }

    private void merge(SplitBrainMergeEntryView<Data, Data> mergingEntry) {
        Data dataKey = mergingEntry.getKey();
        Data oldValue = hasMapListener ? getValue(dataKey) : null;

        //noinspection unchecked
        if (Boolean.TRUE.equals(recordStore.merge(mergingEntry, mergePolicy))) {
            hasMergedValues = true;
            Data dataValue = getValueOrPostProcessedValue(dataKey, getValue(dataKey));
            mapServiceContext.interceptAfterPut(name, dataValue);

            if (hasMapListener) {
                mapEventPublisher.publishEvent(getCallerAddress(), name, MERGED, dataKey, oldValue, dataValue);
            }
            if (hasWanReplication) {
                EntryView entryView = createSimpleEntryView(dataKey, dataValue, recordStore.getRecord(dataKey));
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
            if (hasBackups) {
                mapEntries.add(dataKey, dataValue);
                backupRecordInfos.add(buildRecordInfo(recordStore.getRecord(dataKey)));
            }
            evict(dataKey);
            if (hasInvalidation) {
                invalidationKeys.add(dataKey);
            }
        }
    }

    private Data getValueOrPostProcessedValue(Data dataKey, Data dataValue) {
        if (!isPostProcessing(recordStore)) {
            return dataValue;
        }
        Record record = recordStore.getRecord(dataKey);
        return mapServiceContext.toData(record.getValue());
    }

    private Data getValue(Data dataKey) {
        Record record = recordStore.getRecord(dataKey);
        if (record != null) {
            return mapServiceContext.toData(record.getValue());
        }
        return null;
    }

    @Override
    public Object getResponse() {
        return hasMergedValues;
    }

    @Override
    public boolean shouldBackup() {
        return hasBackups && !backupRecordInfos.isEmpty();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public void afterRun() throws Exception {
        invalidateNearCache(invalidationKeys);

        super.afterRun();
    }

    @Override
    public Operation getBackupOperation() {
        return new HDPutAllBackupOperation(name, mapEntries, backupRecordInfos);
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
        return EnterpriseMapDataSerializerHook.MERGE;
    }
}
