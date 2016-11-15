package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Iterates and return a copy of a map's partition data to be used by WAN replication.
 */
public class GetMapPartitionDataOperation extends MapOperation implements ReadonlyOperation {

    private Set<RecordReplicationInfo> recordSet;

    public GetMapPartitionDataOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        recordSet = new HashSet<RecordReplicationInfo>(recordStore.size());
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            RecordReplicationInfo recordReplicationInfo
                    = createRecordReplicationInfo(key, record, mapServiceContext);
            recordSet.add(recordReplicationInfo);
        }
    }

    private RecordReplicationInfo createRecordReplicationInfo(Data key, Record record, MapServiceContext mapServiceContext) {
        RecordInfo info = buildRecordInfo(record);
        return new RecordReplicationInfo(mapServiceContext.toData(key), mapServiceContext.toData(record.getValue()), info);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return recordSet;
    }

}
