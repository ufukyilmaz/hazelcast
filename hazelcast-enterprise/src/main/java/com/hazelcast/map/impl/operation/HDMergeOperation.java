package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.MERGED;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Contains multiple merge entries for split-brain healing with with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class HDMergeOperation extends HDMapOperation
        implements PartitionAwareOperation, BackupAwareOperation {

    private List<MapMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy;

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

    HDMergeOperation(String name, List<MapMergeTypes> mergingEntries, SplitBrainMergePolicy<Data, MapMergeTypes> policy,
                     boolean disableWanReplicationEvent) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = policy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        size = mergingEntries.size();

        hasMapListener = mapEventPublisher.hasEventListener(name);
        hasWanReplication = mapContainer.isWanReplicationEnabled() && !disableWanReplicationEvent;
        hasBackups = mapContainer.getTotalBackupCount() > 0;
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            mapEntries = new MapEntries(size);
            backupRecordInfos = new ArrayList<>(size);
        }
        if (hasInvalidation) {
            invalidationKeys = new ArrayList<>(size);
        }
    }

    @Override
    public void runInternal() {
        // if currentIndex is not zero, this is a continuation of the operation after a NativeOOME
        while (currentIndex < size) {
            merge(mergingEntries.get(currentIndex));
            currentIndex++;
        }
    }

    private void merge(MapMergeTypes mergingEntry) {
        Data dataKey = mergingEntry.getKey();
        Data oldValue = hasMapListener ? getValue(dataKey) : null;

        if (recordStore.merge(mergingEntry, mergePolicy, getCallerProvenance())) {
            hasMergedValues = true;
            Data dataValue = getValueOrPostProcessedValue(dataKey, getValue(dataKey));
            mapServiceContext.interceptAfterPut(name, dataValue);

            if (hasMapListener) {
                mapEventPublisher.publishEvent(getCallerAddress(), name, MERGED, dataKey, oldValue, dataValue);
            }
            if (hasWanReplication) {
                publishWanUpdate(dataKey, dataValue);
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
        return new HDPutAllBackupOperation(name, mapEntries, backupRecordInfos, disableWanReplicationEvent);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergingEntries.size());
        for (MapMergeTypes mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergingEntries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            MapMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MERGE;
    }
}
