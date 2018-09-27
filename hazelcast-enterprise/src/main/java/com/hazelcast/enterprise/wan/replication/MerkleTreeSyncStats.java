package com.hazelcast.enterprise.wan.replication;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

class MerkleTreeSyncStats {
    private final long syncStartNanos = System.nanoTime();

    private long syncDurationNanos;
    private int partitionsSynced;
    private int nodesSynced;
    private long recordsSynced;
    private int minLeafEntryCount = Integer.MAX_VALUE;
    private int maxLeafEntryCount = Integer.MIN_VALUE;
    private double avgEntriesPerLeaf;
    private double stdDevEntriesPerLeaf;
    private long sumEntryCountSquares;

    void onSyncPartition() {
        partitionsSynced++;
    }

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

    void onSyncComplete() {
        syncDurationNanos = System.nanoTime() - syncStartNanos;
    }

    long getSyncDurationSecs() {
        return NANOSECONDS.toSeconds(syncDurationNanos);
    }

    int getPartitionsSynced() {
        return partitionsSynced;
    }

    int getNodesSynced() {
        return nodesSynced;
    }

    long getRecordsSynced() {
        return recordsSynced;
    }

    int getMinLeafEntryCount() {
        return minLeafEntryCount;
    }

    int getMaxLeafEntryCount() {
        return maxLeafEntryCount;
    }

    double getAvgEntriesPerLeaf() {
        return avgEntriesPerLeaf;
    }

    double getStdDevEntriesPerLeaf() {
        return stdDevEntriesPerLeaf;
    }
}
