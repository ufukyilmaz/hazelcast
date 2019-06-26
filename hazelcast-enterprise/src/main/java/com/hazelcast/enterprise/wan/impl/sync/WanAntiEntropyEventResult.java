package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.SetUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Result of {@link WanAntiEntropyEventPublishOperation}
 */
public class WanAntiEntropyEventResult implements IdentifiedDataSerializable {
    private Set<Integer> processedPartitions = new HashSet<Integer>();

    public WanAntiEntropyEventResult() {
    }

    public Set<Integer> getProcessedPartitions() {
        return processedPartitions;
    }

    public void addProcessedPartitions(Collection<Integer> processedPartitions) {
        this.processedPartitions.addAll(processedPartitions);
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_ANTI_ENTROPY_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(processedPartitions.size());
        for (Integer processedPartition : processedPartitions) {
            out.writeInt(processedPartition);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        processedPartitions = SetUtil.createHashSet(size);
        for (int i = 0; i < size; i++) {
            processedPartitions.add(in.readInt());
        }
    }
}
