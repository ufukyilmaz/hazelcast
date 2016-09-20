package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import java.io.IOException;

public class HDMapFetchEntriesOperation extends HDMapOperation {

    private int lastTableIndex;
    private int fetchSize;
    private transient MapEntriesWithCursor result;

    public HDMapFetchEntriesOperation() {
    }

    public HDMapFetchEntriesOperation(String name, int lastTableIndex, int fetchSize) {
        super(name);
        this.lastTableIndex = lastTableIndex;
        this.fetchSize = fetchSize;
    }

    @Override
    protected void runInternal() {
        result = recordStore.fetchEntries(lastTableIndex, fetchSize);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        lastTableIndex = in.readInt();
        fetchSize = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(lastTableIndex);
        out.writeInt(fetchSize);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.FETCH_ENTRIES;
    }
}
