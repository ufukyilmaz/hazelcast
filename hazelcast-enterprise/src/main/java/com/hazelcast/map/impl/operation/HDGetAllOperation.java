package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HDGetAllOperation extends HDMapOperation implements ReadonlyOperation, PartitionAwareOperation {

    private List<Data> keys = new ArrayList<>();
    private MapEntries entries;

    public HDGetAllOperation() {
    }

    public HDGetAllOperation(String name, List<Data> keys) {
        super(name);
        this.keys = keys;
    }

    @Override
    protected void runInternal() {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitionId = getPartitionId();
        recordStore = mapService.getMapServiceContext().getRecordStore(partitionId, name);
        Set<Data> partitionKeySet = new HashSet<>(keys.size());
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        entries = recordStore.getAll(partitionKeySet, getCallerAddress());
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = in.readData();
                keys.add(data);
            }
        }
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.GET_ALL;
    }
}
