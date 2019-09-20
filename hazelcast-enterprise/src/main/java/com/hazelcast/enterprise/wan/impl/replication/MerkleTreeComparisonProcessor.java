package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.impl.operation.WanMerkleTreeNodeCompareOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.wan.impl.merkletree.MerkleTreeUtil;

import java.util.Map;

import static com.hazelcast.enterprise.wan.impl.replication.ComparisonState.ABORTED;
import static com.hazelcast.enterprise.wan.impl.replication.ComparisonState.FINISHED;
import static com.hazelcast.enterprise.wan.impl.replication.ComparisonState.IN_PROGRESS;

/**
 * Class responsible for processing local and remote Merkle tree comparison results
 */
class MerkleTreeComparisonProcessor {
    private ILogger logger = Logger.getLogger(MerkleTreeComparisonProcessor.class);

    private ComparisonState comparisonState = IN_PROGRESS;
    private Map<Integer, int[]> lastLocalNodeValues;
    private Map<Integer, int[]> lastRemoteNodeValues;
    private Map<Integer, int[]> difference;
    private int localTreeLevel = -1;
    private int remoteTreeLevel = -1;

    /**
     * Processes local Merkle tree comparison result.
     *
     * @param localNodeValues The node order-value pairs returned by local execution of
     *                        {@link WanMerkleTreeNodeCompareOperation}
     */
    void processLocalNodeValues(Map<Integer, int[]> localNodeValues) {
        assert comparisonState == IN_PROGRESS;

        lastLocalNodeValues = localNodeValues;
        localTreeLevel = extractTreeLevel(localNodeValues, localTreeLevel);

        if (localNodeValues.containsValue(null)) {
            // the last compared remote level doesn't exist in the local tree, the remote tree is deeper
            // we've found the difference
            difference = localize(lastRemoteNodeValues, localTreeLevel);
            comparisonState = FINISHED;

        } else if (localTreeLevel == remoteTreeLevel) {
            // the comparison reached the leaf level level in the local cluster
            // we've found the difference
            difference = localNodeValues;
            comparisonState = FINISHED;

        } else if (localNodeValues.isEmpty()) {
            // the map in the source and target clusters is identical
            // may happen if the local map is updated during the comparison
            // and becomes identical with the target
            difference = localNodeValues;
            comparisonState = FINISHED;
        }
    }

    /**
     * Processes remote Merkle tree comparison result.
     *
     * @param remoteNodeValues The node order-value pairs returned by executing
     *                         {@link WanMerkleTreeNodeCompareOperation}
     *                         on the target cluster
     */
    void processRemoteNodeValues(Map<Integer, int[]> remoteNodeValues) {
        assert comparisonState == IN_PROGRESS;

        lastRemoteNodeValues = remoteNodeValues;

        if (remoteNodeValues == null) {
            // connection manager is shutting down, aborting merkle tree sync
            comparisonState = ABORTED;
            logger.fine("Connection manager is shutting down, aborting merkle tree sync");
            return;
        }

        remoteTreeLevel = extractTreeLevel(remoteNodeValues, remoteTreeLevel);

        if (remoteNodeValues.containsValue(null)) {
            // the last compared local level doesn't exist in the remote tree, the local tree is deeper
            // we've found the difference
            difference = lastLocalNodeValues;
            comparisonState = FINISHED;

        } else if (localTreeLevel == remoteTreeLevel) {
            // the comparison reached the leaf level on the target
            // we've found the difference
            difference = remoteNodeValues;
            comparisonState = FINISHED;

        } else if (remoteNodeValues.isEmpty()) {
            // the map in the source and target clusters is identical
            difference = remoteNodeValues;
            comparisonState = FINISHED;
        }
    }

    /**
     * Checks if the Merkle tree comparison is finished.
     * <p>
     * The comparison is treated finished if the difference between the local and the target Merkle trees are found, or the
     * comparision is aborted by shutting down the {@link WanConnectionManager}
     *
     * @return <code>true</code> if the comparison finished, <code>false</code> otherwise
     */
    boolean isComparisonFinished() {
        return comparisonState == FINISHED || comparisonState == ABORTED;
    }

    /**
     * Returns the difference found by the Merkle tree comparison
     *
     * @return the difference
     */
    Map<Integer, int[]> getDifference() {
        return difference;
    }

    /**
     * Returns the actual state of the comparison
     *
     * @return the actual state of the comparison
     */
    ComparisonState getComparisonState() {
        return comparisonState;
    }

    private static int extractTreeLevel(Map<Integer, int[]> nodeValues, int currentLevel) {
        for (int[] diffs : nodeValues.values()) {
            if (diffs != null && diffs.length > 0) {
                // we take the first node order from which we can tell
                // which level the comparison is on
                return MerkleTreeUtil.getLevelOfNode(diffs[0]);
            }
        }
        return currentLevel;
    }

    /**
     * Replaces the remote node hash sequence with the parents' sequence
     * if the remote nodes are on deeper level than the local leafLevel.
     *
     * @param diff      The original map with the remote node hash sequences
     * @param leafLevel The leaf level to which localize the map
     * @return the map with localized node hash sequences
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private Map<Integer, int[]> localize(Map<Integer, int[]> diff, int leafLevel) {
        Map<Integer, int[]> localizedDiff = MapUtil.createHashMap(diff.size());
        for (Map.Entry<Integer, int[]> entry : diff.entrySet()) {
            int[] nodeValues = entry.getValue();
            if (nodeValues != null && nodeValues.length > 0 && MerkleTreeUtil.getLevelOfNode(nodeValues[0]) > leafLevel) {
                int[] localizedNodeValues = new int[nodeValues.length / 2];
                int localizedIdx = 0;
                for (int i = 0; i < nodeValues.length; i += 4) {
                    int leftChildOrder = nodeValues[i];
                    int leftChildHash = nodeValues[i + 1];
                    int rightChildHash = nodeValues[i + 3];
                    int parentChildOrder = MerkleTreeUtil.getParentOrder(leftChildOrder);

                    localizedNodeValues[localizedIdx++] = parentChildOrder;
                    localizedNodeValues[localizedIdx++] = MerkleTreeUtil.sumHash(leftChildHash, rightChildHash);
                }
                localizedDiff.put(entry.getKey(), localizedNodeValues);
            } else {
                localizedDiff.put(entry.getKey(), entry.getValue());
            }
        }
        return localizedDiff;
    }

}

enum ComparisonState {
    IN_PROGRESS,
    FINISHED,
    ABORTED
}
