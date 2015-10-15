package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * Created by emrah on 18/09/15.
 */
public class WanOriginatedMergeOperation extends MergeOperation {

    public WanOriginatedMergeOperation() {

    }

    public WanOriginatedMergeOperation(String mapName, Data data, EntryView<Data, Data> entryView, MapMergePolicy mergePolicy) {
        super(mapName, data, entryView, mergePolicy);
    }

    @Override
    public Operation getBackupOperation() {
        if (dataValue == null) {
            return new RemoveBackupOperation(name, dataKey, false, true);
        } else {
            final Record record = recordStore.getRecord(dataKey);
            final RecordInfo replicationInfo = record != null ? Records.buildRecordInfo(record) : null;
            return new PutBackupOperation(name, dataKey, dataValue, replicationInfo, false, false, true);
        }
    }

}
