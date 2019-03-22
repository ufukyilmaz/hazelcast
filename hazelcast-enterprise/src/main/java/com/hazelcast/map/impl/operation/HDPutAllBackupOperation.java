package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

public class HDPutAllBackupOperation extends HDMapOperation
        implements PartitionAwareOperation, BackupOperation, Versioned {

    private MapEntries entries;
    private List<RecordInfo> recordInfos;

    public HDPutAllBackupOperation(String name, MapEntries entries,
                                   List<RecordInfo> recordInfos, boolean disableWanReplicationEvent) {
        super(name);
        this.entries = entries;
        this.recordInfos = recordInfos;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @SuppressWarnings("unused")
    public HDPutAllBackupOperation() {
    }

    @Override
    protected void runInternal() {
        for (int i = 0; i < entries.size(); i++) {
            Data dataKey = entries.getKey(i);
            Data dataValue = entries.getValue(i);
            Record record = recordStore.putBackup(dataKey, dataValue, getCallerProvenance());
            applyRecordInfo(record, recordInfos.get(i));

            publishWanUpdate(dataKey, dataValue);
            evict(dataKey);
        }
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        entries.writeData(out);
        for (RecordInfo recordInfo : recordInfos) {
            recordInfo.writeData(out);
        }
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        entries = new MapEntries();
        entries.readData(in);
        int size = entries.size();
        recordInfos = new ArrayList<RecordInfo>(size);
        for (int i = 0; i < size; i++) {
            RecordInfo recordInfo = new RecordInfo();
            recordInfo.readData(in);
            recordInfos.add(recordInfo);
        }
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
