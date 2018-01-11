package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

public class HDPutAllBackupOperation extends HDMapOperation implements PartitionAwareOperation, BackupOperation {

    private MapEntries entries;
    private List<RecordInfo> recordInfos;

    public HDPutAllBackupOperation(String name, MapEntries entries, List<RecordInfo> recordInfos) {
        super(name);
        this.entries = entries;
        this.recordInfos = recordInfos;
    }

    @SuppressWarnings("unused")
    public HDPutAllBackupOperation() {
    }

    @Override
    protected void runInternal() {
        boolean wanEnabled = mapContainer.isWanReplicationEnabled();
        for (int i = 0; i < entries.size(); i++) {
            Data dataKey = entries.getKey(i);
            Data dataValue = entries.getValue(i);
            Record record = recordStore.putBackup(dataKey, dataValue);
            applyRecordInfo(record, recordInfos.get(i));
            if (wanEnabled) {
                Data dataValueAsData = mapServiceContext.toData(dataValue);
                EntryView entryView = createSimpleEntryView(dataKey, dataValueAsData, record);
                mapEventPublisher.publishWanReplicationUpdateBackup(name, entryView);
            }

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
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
