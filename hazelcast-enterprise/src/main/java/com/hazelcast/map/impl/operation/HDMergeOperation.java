package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class HDMergeOperation extends HDBasePutOperation {

    private MapMergePolicy mergePolicy;
    private EntryView<Data, Data> mergingEntry;
    private boolean disableWanReplicationEvent;

    private transient boolean merged;

    public HDMergeOperation() {
    }

    public HDMergeOperation(String name, EntryView<Data, Data> entryView,
                            MapMergePolicy policy, boolean disableWanReplicationEvent) {
        super(name, entryView.getKey(), null);
        this.mergingEntry = entryView;
        this.mergePolicy = policy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    protected void runInternal() {
        Record oldRecord = recordStore.getRecord(dataKey);
        if (oldRecord != null) {
            dataOldValue = mapServiceContext.toData(oldRecord.getValue());
        }
        merged = recordStore.merge(dataKey, mergingEntry, mergePolicy);
        if (merged) {
            Record record = recordStore.getRecord(dataKey);
            if (record != null) {
                dataValue = mapServiceContext.toData(record.getValue());
                dataMergingValue = mapServiceContext.toData(mergingEntry.getValue());
            }
        }
    }

    @Override
    protected boolean canThisOpGenerateWANEvent() {
        return !disableWanReplicationEvent;
    }

    @Override
    public Object getResponse() {
        return merged;
    }

    @Override
    public boolean shouldBackup() {
        return merged;
    }

    @Override
    public void afterRun() throws Exception {
        if (merged) {
            eventType = EntryEventType.MERGED;
            super.afterRun();
        }

        disposeDeferredBlocks();
    }

    @Override
    public Operation getBackupOperation() {
        if (dataValue == null) {
            return new HDRemoveBackupOperation(name, dataKey, false, disableWanReplicationEvent);
        } else {
            final Record record = recordStore.getRecord(dataKey);
            final RecordInfo replicationInfo = Records.buildRecordInfo(record);
            return new HDPutBackupOperation(name, dataKey, dataValue, replicationInfo, false, false, disableWanReplicationEvent);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
        mergePolicy = in.readObject();
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MERGE;
    }
}
