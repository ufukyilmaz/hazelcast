package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class HDLegacyMergeOperation extends HDBasePutOperation {

    private MapMergePolicy mergePolicy;
    private EntryView<Data, Data> mergingEntry;

    private transient boolean merged;

    public HDLegacyMergeOperation() {
    }

    public HDLegacyMergeOperation(String name, EntryView<Data, Data> entryView,
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
            oldValue = mapServiceContext.toData(oldRecord.getValue());
        }
        merged = recordStore.merge(dataKey, mergingEntry, mergePolicy, getCallerProvenance());
        if (merged) {
            Record record = recordStore.getRecord(dataKey);
            if (record != null) {
                dataValue = mapServiceContext.toData(record.getValue());
                dataMergingValue = mapServiceContext.toData(mergingEntry.getValue());
            }
        }
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
            return super.getBackupOperation();
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
        return EnterpriseMapDataSerializerHook.LEGACY_MERGE;
    }
}
