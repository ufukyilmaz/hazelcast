package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.wan.impl.merkletree.MerkleTree;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

import static com.hazelcast.wan.impl.merkletree.MerkleTreeUtil.getLeafOrderForHash;
import static com.hazelcast.wan.impl.merkletree.MerkleTreeUtil.getLevelOfNode;

/**
 * Operation that queries the number of the keys under a set of Merkle tree nodes
 */
public class MerkleTreeGetEntryCountOperation extends MapOperation implements ReadonlyOperation {
    /**
     * The Merkle tree nodes for which we query the key counts
     */
    private int[] merkleTreeNodeOrders;

    /**
     * Sum of the keys under the provided Merkle tree nodes
     */
    private int result;

    public MerkleTreeGetEntryCountOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MerkleTreeGetEntryCountOperation(String mapName, int[] merkleTreeNodeOrders) {
        super(mapName);
        this.merkleTreeNodeOrders = merkleTreeNodeOrders;
    }

    @Override
    protected void runInternal() {
        int partitionId = getPartitionId();
        EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) mapServiceContext
                .getPartitionContainer(partitionId);
        MerkleTree localMerkleTree = partitionContainer.getMerkleTreeOrNull(getName());

        if (localMerkleTree == null
                || merkleTreeNodeOrders == null
                || merkleTreeNodeOrders.length == 0) {
            result = 0;
            return;
        }

        BitSet nodeOrderBitSet = new BitSet(Arrays.stream(merkleTreeNodeOrders).max().getAsInt());
        for (int order : merkleTreeNodeOrders) {
            nodeOrderBitSet.set(order);
        }
        // here we assume all nodes are on the same level
        int levelOfRequestedNodes = getLevelOfNode(merkleTreeNodeOrders[0]);

        recordStore.iterator()
                   .forEachRemaining(entry -> {
                       int keyHash = entry.getKey().hashCode();
                       int currentKeyNodeOrder = getLeafOrderForHash(keyHash, levelOfRequestedNodes);
                       if (nodeOrderBitSet.get(currentKeyNodeOrder)) {
                           result++;
                       }
                   });
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EnterpriseMapDataSerializerHook.MERKLE_TREE_GET_ENTRY_COUNT_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        merkleTreeNodeOrders = in.readIntArray();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeIntArray(merkleTreeNodeOrders);
    }

}
