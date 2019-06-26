package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.util.SetUtil;

import java.util.Iterator;
import java.util.Set;

/**
 * Iterates and return a copy of a map's partition data to be used by WAN replication.
 */
public class GetMapPartitionDataOperation extends MapOperation implements ReadonlyOperation {

    private Set<SimpleEntryView> recordSet;

    public GetMapPartitionDataOperation() {
    }

    public GetMapPartitionDataOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordSet = SetUtil.createHashSet(recordStore.size());
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            recordSet.add(createSimpleEntryView(record));
        }
    }

    private SimpleEntryView<Object, Object> createSimpleEntryView(Record record) {
        SimpleEntryView<Object, Object> simpleEntryView = new SimpleEntryView<Object, Object>(
                mapServiceContext.toData(record.getKey()), mapServiceContext.toData(record.getValue()));
        simpleEntryView.setVersion(record.getVersion());
        simpleEntryView.setHits(record.getHits());
        simpleEntryView.setLastAccessTime(record.getLastAccessTime());
        simpleEntryView.setLastUpdateTime(record.getLastUpdateTime());
        simpleEntryView.setTtl(record.getTtl());
        simpleEntryView.setCreationTime(record.getCreationTime());
        simpleEntryView.setExpirationTime(record.getExpirationTime());
        simpleEntryView.setLastStoredTime(record.getLastStoredTime());
        return simpleEntryView;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return recordSet;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.GET_MAP_PARTITION_DATA_OPERATION;
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }
}
