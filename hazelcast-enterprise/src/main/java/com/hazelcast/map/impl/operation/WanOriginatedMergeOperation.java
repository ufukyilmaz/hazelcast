package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * Incoming {@link com.hazelcast.map.impl.wan.MapReplicationUpdate} events are merged using this operation.
 * This operation is needed to prevent wan event generation of backup operations of {@link MergeOperation}
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
            final RecordInfo replicationInfo = Records.buildRecordInfo(record);
            return new PutBackupOperation(name, dataKey, dataValue, replicationInfo, false, false, true);
        }
    }
}
