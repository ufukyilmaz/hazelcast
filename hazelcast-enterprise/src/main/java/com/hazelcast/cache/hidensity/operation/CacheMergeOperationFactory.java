package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
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
    private List<SplitBrainMergeEntryView<Data, Data>>[] mergeEntries;
    private SplitBrainMergePolicy policy;

    public CacheMergeOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public CacheMergeOperationFactory(String name, int[] partitions, List<SplitBrainMergeEntryView<Data, Data>>[] mergeEntries,
                                      SplitBrainMergePolicy policy) {
        this.name = name;
        this.partitions = partitions;
        this.mergeEntries = mergeEntries;
        this.policy = policy;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new CacheMergeOperation(name, mergeEntries[i], policy);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeIntArray(partitions);
        for (List<SplitBrainMergeEntryView<Data, Data>> entry : mergeEntries) {
            out.writeInt(entry.size());
            for (SplitBrainMergeEntryView<Data, Data> mergeEntry : entry) {
                out.writeObject(mergeEntry);
            }
        }
        out.writeObject(policy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitions = in.readIntArray();
        //noinspection unchecked
        mergeEntries = new List[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            int size = in.readInt();
            List<SplitBrainMergeEntryView<Data, Data>> list = new ArrayList<SplitBrainMergeEntryView<Data, Data>>(size);
            for (int j = 0; j < size; j++) {
                SplitBrainMergeEntryView<Data, Data> mergeEntry = in.readObject();
                list.add(mergeEntry);
            }
            mergeEntries[i] = list;
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
