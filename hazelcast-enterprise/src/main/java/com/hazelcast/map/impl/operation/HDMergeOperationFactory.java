package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.List;

/**
 * Inserts the merging entries for all partitions of a member via locally invoked {@link MergeOperation}.
 *
 * @since 3.10
 */
public class HDMergeOperationFactory extends MergeOperationFactory {

    public HDMergeOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public HDMergeOperationFactory(String name, int[] partitions, List<MapMergeTypes>[] mergingEntries,
                                   SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy) {
        this.name = name;
        this.partitions = partitions;
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new HDMergeOperation(name, mergingEntries[i], mergePolicy, false);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MERGE_FACTORY;
    }
}
