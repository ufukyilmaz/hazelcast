package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
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
    private List<CacheMergeTypes<Object, Object>>[] mergingEntries;
    private SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> policy;

    public CacheMergeOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public CacheMergeOperationFactory(String name, int[] partitions, List<CacheMergeTypes<Object, Object>>[] mergingEntries,
                                      SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> policy) {
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
        out.writeString(name);
        out.writeIntArray(partitions);
        for (List<CacheMergeTypes<Object, Object>> list : mergingEntries) {
            out.writeInt(list.size());
            for (CacheMergeTypes mergingEntry : list) {
                out.writeObject(mergingEntry);
            }
        }
        out.writeObject(policy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitions = in.readIntArray();
        //noinspection unchecked
        mergingEntries = new List[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            int size = in.readInt();
            List<CacheMergeTypes<Object, Object>> list = new ArrayList<>(size);
            for (int j = 0; j < size; j++) {
                CacheMergeTypes mergingEntry = in.readObject();
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
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.MERGE_FACTORY;
    }
}
