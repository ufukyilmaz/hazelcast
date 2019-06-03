package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * @deprecated Not used.
 */
@Deprecated
public final class CacheIterationResult implements IdentifiedDataSerializable {

    private Data[] keys;
    private Data[] values;
    private int partitionId;
    private int slot;
    private int count;

    public CacheIterationResult() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public CacheIterationResult(Data[] keys, Data[] values, int partitionId, int slot, int count) {
        this.keys = keys;
        this.values = values;
        this.partitionId = partitionId;
        this.slot = slot;
        this.count = count;
    }

    public Data getKey(int ix) {
        return keys[ix];
    }

    public Data getValue(int ix) {
        return values[ix];
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getSlot() {
        return slot;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(slot);
        out.writeInt(count);
        for (int i = 0; i < count; i++) {
            out.writeData(keys[i]);
            out.writeData(values[i]);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        slot = in.readInt();
        count = in.readInt();
        keys = new Data[count];
        values = new Data[count];
        for (int i = 0; i < count; i++) {
            keys[i] = in.readData();
            values[i] = in.readData();
        }
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException("CacheIterationResult is deprecated");
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException("CacheIterationResult is deprecated");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheIterationResult{");
        sb.append("partitionId=").append(partitionId);
        sb.append(", slot=").append(slot);
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
