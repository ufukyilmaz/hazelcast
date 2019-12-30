package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.SetUtil;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.util.Set;

/**
 * Iterates and return a copy of a map's partition data to be used by WAN replication.
 */
public class GetMapPartitionDataOperation extends MapOperation implements ReadonlyOperation {

    private Set<WanMapEntryView<Object, Object>> recordSet;

    public GetMapPartitionDataOperation() {
    }

    public GetMapPartitionDataOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordSet = SetUtil.createHashSet(recordStore.size());
        recordStore.forEach((dataKey, record)
                -> recordSet.add(createWanEntryView(dataKey, record)), getReplicaIndex() != 0);
    }

    private WanMapEntryView<Object, Object> createWanEntryView(Data dataKey, Record<Object> record) {
        return EntryViews.createWanEntryView(
                mapServiceContext.toData(dataKey),
                mapServiceContext.toData(record.getValue()),
                record,
                getNodeEngine().getSerializationService());
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
        return WanDataSerializerHook.GET_MAP_PARTITION_DATA_OPERATION;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }
}
