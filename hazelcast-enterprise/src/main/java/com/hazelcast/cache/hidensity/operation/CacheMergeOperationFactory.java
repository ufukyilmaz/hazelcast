package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.merge.MergingEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Inserts the merging entries for all partitions of a member via locally invoked {@link CacheMergeOperation}.
 *
 * @since 3.10
 */
public class CacheMergeOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private List<MergingEntry<Data, Data>>[] mergingEntries;
    private SplitBrainMergePolicy policy;

    public CacheMergeOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public CacheMergeOperationFactory(String name, int[] partitions, List<MergingEntry<Data, Data>>[] mergingEntries,
                                      SplitBrainMergePolicy policy) {
        this.name = name;
        this.partitions = partitions;
        this.mergingEntries = mergingEntries;
        this.policy = policy;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new CacheMergeOperation(name, mergingEntries[i], policy);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeIntArray(partitions);
        for (List<MergingEntry<Data, Data>> list : mergingEntries) {
            out.writeInt(list.size());
            for (MergingEntry<Data, Data> mergingEntry : list) {
                out.writeObject(mergingEntry);
            }
        }
        out.writeObject(policy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitions = in.readIntArray();
        //noinspection unchecked
        mergingEntries = new List[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            int size = in.readInt();
            List<MergingEntry<Data, Data>> list = new ArrayList<MergingEntry<Data, Data>>(size);
            for (int j = 0; j < size; j++) {
                MergingEntry<Data, Data> mergingEntry = in.readObject();
                list.add(mergingEntry);
            }
            mergingEntries[i] = list;
        }
        policy = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.MERGE_FACTORY;
    }
}
