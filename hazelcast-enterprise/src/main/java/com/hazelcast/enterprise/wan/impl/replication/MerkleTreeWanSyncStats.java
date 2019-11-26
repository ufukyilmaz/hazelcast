package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.wan.impl.WanSyncStats;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Merkle tree specific implementation of the {@link WanSyncStats} interface.
 */
public class MerkleTreeWanSyncStats implements WanSyncStats {
    private final UUID uuid;
    private final long syncStartNanos = System.nanoTime();

    private final int partitionsToSync;
    private final AtomicInteger partitionsSynced = new AtomicInteger();
    private final AtomicInteger recordsSynced = new AtomicInteger();
    private final AtomicInteger nodesSynced = new AtomicInteger();
    private final AtomicLong sumEntryCountSquares = new AtomicLong();

    private volatile long syncDurationNanos;
    private volatile int minLeafEntryCount = Integer.MAX_VALUE;
    private volatile int maxLeafEntryCount = Integer.MIN_VALUE;
    private volatile double avgEntriesPerLeaf;
    private volatile double stdDevEntriesPerLeaf;

    MerkleTreeWanSyncStats(UUID uuid, int partitionsToSync) {
        this.uuid = uuid;
        this.partitionsToSync = partitionsToSync;
    }

    /**
     * Callback for synchronizing a partition.
     *
     * @return the number of partitions synced
     */
    int onSyncPartition() {
        return partitionsSynced.incrementAndGet();
    }

    /**
     * Callback for synchronizing a Merkle tree node.
     *
     * @param leafEntryCount the number of the records belong to the
     *                       synchronized Merkle tree node
     *
     * @return the number of records synchronized
     */
    void onSyncLeaf(int leafEntryCount) {
        int recordsSynchronized = recordsSynced.addAndGet(leafEntryCount);
        int nodesSynchronized = nodesSynced.incrementAndGet();

        if (leafEntryCount < minLeafEntryCount) {
            minLeafEntryCount = leafEntryCount;
        }

        if (leafEntryCount > maxLeafEntryCount) {
            maxLeafEntryCount = leafEntryCount;
        }

        avgEntriesPerLeaf = nodesSynchronized != 0 ? (double) recordsSynchronized / nodesSynchronized : 0;

        long sumSquares = sumEntryCountSquares.addAndGet((long) leafEntryCount * leafEntryCount);
        double avgSquare = avgEntriesPerLeaf * avgEntriesPerLeaf;
        stdDevEntriesPerLeaf = Math.sqrt((((double) sumSquares) / nodesSynchronized) - avgSquare);
    }

    /**
     * Callback for completing synchronization
     */
    void onSyncComplete() {
        syncDurationNanos = System.nanoTime() - syncStartNanos;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public long getDurationSecs() {
        return NANOSECONDS.toSeconds(syncDurationNanos);
    }

    @Override
    public int getPartitionsToSync() {
        return partitionsToSync;
    }

    @Override
    public int getPartitionsSynced() {
        return partitionsSynced.get();
    }

    @Override
    public int getRecordsSynced() {
        return recordsSynced.get();
    }

    /**
     * Returns the number of the synchronized Merkle tree nodes
     */
    public int getNodesSynced() {
        return nodesSynced.get();
    }

    /**
     * Returns the minimum of the number of records belong the synchronized
     * Merkle tree nodes have
     */
    public int getMinLeafEntryCount() {
        return partitionsToSync != 0 ? minLeafEntryCount : 0;
    }

    /**
     * Returns the maximum of the number of records belong the synchronized
     * Merkle tree nodes have
     */
    public int getMaxLeafEntryCount() {
        return partitionsToSync != 0 ? maxLeafEntryCount : 0;
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
