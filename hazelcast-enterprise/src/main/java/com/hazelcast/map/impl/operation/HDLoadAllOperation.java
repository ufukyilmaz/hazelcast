package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Triggers map store load of all given keys.
 */
public class HDLoadAllOperation extends HDMapOperation implements PartitionAwareOperation, MutatingOperation {

    private List<Data> keys;

    private boolean replaceExistingValues;

    public HDLoadAllOperation() {
        keys = Collections.emptyList();
    }

    public HDLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    protected void runInternal() {
        keys = selectThisPartitionsKeys(this.keys);
        recordStore.loadAllFromStore(keys, replaceExistingValues);
    }
    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        invalidateNearCache(keys);

        disposeDeferredBlocks();
    }

    private List<Data> selectThisPartitionsKeys(Collection<Data> keys) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        final int partitionId = getPartitionId();
        List<Data> dataKeys = null;
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                if (dataKeys == null) {
                    dataKeys = new ArrayList<>();
                }
                dataKeys.add(key);
            }
        }
        if (dataKeys == null) {
            return Collections.emptyList();
        }
        return dataKeys;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            out.writeData(key);
        }
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<>(size);
        }
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
        replaceExistingValues = in.readBoolean();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", replaceExistingValues=").append(replaceExistingValues);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.LOAD_ALL;
    }
}
