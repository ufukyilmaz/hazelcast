package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.MapUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * The result of the merkle tree node value comparison mechanism.
 *
 * @see com.hazelcast.wan.merkletree.MerkleTree
 * @since 3.11
 */
public class MerkleTreeNodeValueComparison implements IdentifiedDataSerializable {
    private Map<Integer, int[]> merkleTreeNodeValues = Collections.emptyMap();

    public MerkleTreeNodeValueComparison() {
    }

    public MerkleTreeNodeValueComparison(Map<Integer, int[]> merkleTreeNodeValues) {
        this.merkleTreeNodeValues = merkleTreeNodeValues;
    }

    /**
     * Returns the merkle tree node order-value pairs for the comparison of the
     * given {@code partitionId}.
     *
     * @param partitionId the partition ID of the comparison
     * @return the node order-value pairs
     */
    public int[] getMerkleTreeNodeValues(int partitionId) {
        return merkleTreeNodeValues.get(partitionId);
    }

    /**
     * Returns the partition IDs covered by this merkle tree comparison.
     *
     * @return the set of partition IDs
     */
    public Set<Integer> getPartitionIds() {
        return merkleTreeNodeValues.keySet();
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.MERKLE_TREE_NODE_VALUE_COMPARISON;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(merkleTreeNodeValues.size());
        for (Entry<Integer, int[]> partitionNodes : merkleTreeNodeValues.entrySet()) {
            out.writeInt(partitionNodes.getKey());
            out.writeIntArray(partitionNodes.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int partitionCount = in.readInt();
        merkleTreeNodeValues = MapUtil.createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            merkleTreeNodeValues.put(in.readInt(), in.readIntArray());
        }
    }
}
