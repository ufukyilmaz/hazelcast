package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.wan.impl.operation.MerkleTreeNodeValueComparison;
import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.wan.impl.merkletree.MerkleTree;
import com.hazelcast.wan.impl.merkletree.MerkleTreeUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.wan.impl.merkletree.MerkleTreeUtil.getLevelOfNode;

/**
 * Operation for comparing the local merkle tree node values with the remote
 * node values.
 *
 * @see MerkleTree
 * @since 3.11
 */
public class MerkleTreeNodeCompareOperation extends MapOperation implements ReadonlyOperation {
    private MerkleTreeNodeValueComparison remoteNodes;
    private int[] responseDifferentNodeOrderHashPairs;

    public MerkleTreeNodeCompareOperation() {
    }

    public MerkleTreeNodeCompareOperation(String name, MerkleTreeNodeValueComparison remoteNodes) {
        super(name);
        this.remoteNodes = remoteNodes;
    }

    @Override
    protected void runInternal() {
        int partitionId = getPartitionId();
        MerkleTree localMerkleTree = getMerkleTree(partitionId);

        int[] remotePartitionNodes = remoteNodes != null
                ? remoteNodes.getMerkleTreeNodeValues(partitionId)
                : new int[0];

        if (remotePartitionNodes.length == 0) {
            // special case, we need to return the root
            int rootValue = localMerkleTree != null ? localMerkleTree.getNodeHash(0) : 0;
            responseDifferentNodeOrderHashPairs = new int[]{0, rootValue};
            return;
        }

        if (localMerkleTree == null) {
            // there is no local merkle tree for this partition
            // so all nodes are different
            responseDifferentNodeOrderHashPairs = null;
            return;
        }

        int localTreeDepth = localMerkleTree.depth();
        int remoteLevel = MerkleTreeUtil.getLevelOfNode(remotePartitionNodes[0]);
        if (remoteLevel > localTreeDepth - 1) {
            // the remote level is deeper than our local tree depth
            // we can't compare so all nodes are different
            responseDifferentNodeOrderHashPairs = null;
            return;
        }

        this.responseDifferentNodeOrderHashPairs = compareTrees(localMerkleTree, remotePartitionNodes);
    }

    private MerkleTree getMerkleTree(int partitionId) {
        EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) mapServiceContext
                .getPartitionContainer(partitionId);
        return partitionContainer.getMerkleTreeOrNull(getName());
    }

    /**
     * Compares the local {@link MerkleTree} with the provided remote merkle
     * tree nodes. The remote nodes are given as merkle tree node order-value
     * pairs.
     * All of the nodes in the {@code remotePartitionNodes} should be on the
     * same level of the merkle tree.
     * This method returns the local merkle tree node order-value pairs which
     * are different than the provided nodes. If there is a level deeper than
     * the one provided by the remote nodes, it will return the nodes from that
     * level, otherwise it returns nodes from the same level as the nodes in
     * {@code remotePartitionNodes}.
     *
     * @param localMerkleTree      the local merkle tree
     * @param remotePartitionNodes the order-value node pairs for the remote merkle tree
     * @return the order-value node pairs of the local tree which are different
     * @see MerkleTree
     */
    private int[] compareTrees(MerkleTree localMerkleTree,
                               int[] remotePartitionNodes) {
        ArrayList<Integer> diffNodes = new ArrayList<>();
        int localTreeDepth = localMerkleTree.depth();
        for (int idx = 0; idx < remotePartitionNodes.length; idx += 2) {
            int nodeOrder = remotePartitionNodes[idx];
            int nodeLevel = getLevelOfNode(nodeOrder);
            int remoteNodeValue = remotePartitionNodes[idx + 1];

            int localNodeValue = localMerkleTree.getNodeHash(nodeOrder);
            if (localNodeValue != remoteNodeValue) {
                if (localTreeDepth - 1 == nodeLevel) {
                    // on leaf level, adding the leaf's hash to the differences
                    diffNodes.add(nodeOrder);
                    diffNodes.add(localMerkleTree.getNodeHash(nodeOrder));
                } else {
                    // on node level, adding the hashes of the children to the differences
                    int leftChildOrder = MerkleTreeUtil.getLeftChildOrder(nodeOrder);
                    int rightChildOrder = MerkleTreeUtil.getRightChildOrder(nodeOrder);
                    diffNodes.add(leftChildOrder);
                    diffNodes.add(localMerkleTree.getNodeHash(leftChildOrder));
                    diffNodes.add(rightChildOrder);
                    diffNodes.add(localMerkleTree.getNodeHash(rightChildOrder));
                }
            }
        }

        return asArray(diffNodes);
    }

    private int[] asArray(List<Integer> nodeOrderHashPairs) {
        final int[] nodeOrderHashPairsArr = new int[nodeOrderHashPairs.size()];
        for (int i = 0; i < nodeOrderHashPairs.size(); i++) {
            nodeOrderHashPairsArr[i] = nodeOrderHashPairs.get(i);
        }
        return nodeOrderHashPairsArr;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    public Object getResponse() {
        return responseDifferentNodeOrderHashPairs;
    }


    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EnterpriseMapDataSerializerHook.MERKLE_TREE_NODE_COMPARE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(remoteNodes);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        remoteNodes = in.readObject();
    }
}
