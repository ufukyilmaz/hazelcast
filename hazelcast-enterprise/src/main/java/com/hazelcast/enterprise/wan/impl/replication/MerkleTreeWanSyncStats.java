package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.wan.WanSyncStats;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Merkle tree specific implementation of the {@link WanSyncStats} interface.
 */
public class MerkleTreeWanSyncStats implements WanSyncStats {
    private final long syncStartNanos = System.nanoTime();

    private long syncDurationNanos;
    private int partitionsSynced;
    private int nodesSynced;
    private int recordsSynced;
    private int minLeafEntryCount = Integer.MAX_VALUE;
    private int maxLeafEntryCount = Integer.MIN_VALUE;
    private double avgEntriesPerLeaf;
    private double stdDevEntriesPerLeaf;
    private long sumEntryCountSquares;

    /**
     * Callback for synchronizing a partition
     */
    void onSyncPartition() {
        partitionsSynced++;
    }

    /**
     * Callback for synchronizing a Merkle tree node
     *
     * @param leafEntryCount the number of the records belong to the
     *                       synchronized Merkle tree node
     */
    void onSyncLeaf(int leafEntryCount) {
        recordsSynced += leafEntryCount;
        nodesSynced++;

        if (leafEntryCount < minLeafEntryCount) {
            minLeafEntryCount = leafEntryCount;
        }

        if (leafEntryCount > maxLeafEntryCount) {
            maxLeafEntryCount = leafEntryCount;
        }

        avgEntriesPerLeaf = nodesSynced != 0 ? (double) recordsSynced / nodesSynced : 0;

        sumEntryCountSquares += (long) leafEntryCount * leafEntryCount;
        double avgSquare = avgEntriesPerLeaf * avgEntriesPerLeaf;
        stdDevEntriesPerLeaf = Math.sqrt((((double) sumEntryCountSquares) / nodesSynced) - avgSquare);
    }

    /**
     * Callback for completing synchronization
     */
    void onSyncComplete() {
        syncDurationNanos = System.nanoTime() - syncStartNanos;
    }

    @Override
    public long getDurationSecs() {
        return NANOSECONDS.toSeconds(syncDurationNanos);
    }

    @Override
    public int getPartitionsSynced() {
        return partitionsSynced;
    }

    @Override
    public int getRecordsSynced() {
        return recordsSynced;
    }

    /**
     * Returns the number of the synchronized Merkle tree nodes
     */
    public int getNodesSynced() {
        return nodesSynced;
    }

    /**
     * Returns the minimum of the number of records belong the synchronized
     * Merkle tree nodes have
     */
    public int getMinLeafEntryCount() {
        return minLeafEntryCount;
    }

    /**
     * Returns the maximum of the number of records belong the synchronized
     * Merkle tree nodes have
     */
    public int getMaxLeafEntryCount() {
        return maxLeafEntryCount;
    }

    /**
     * Returns the average of the number of records belong the synchronized
     * Merkle tree nodes have
     */
    public double getAvgEntriesPerLeaf() {
        return avgEntriesPerLeaf;
    }

    /**
     * Returns the standard deviance of the number of records belong the
     * synchronized Merkle tree nodes have
     */
    public double getStdDevEntriesPerLeaf() {
        return stdDevEntriesPerLeaf;
    }
}
