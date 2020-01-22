package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.wan.impl.WanSyncStats;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_AVG_ENTRIES_PER_LEAF;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_MAX_LEAF_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_MIN_LEAF_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_NODES_SYNCED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_PARTITIONS_SYNCED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_PARTITIONS_TO_SYNC;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_RECORDS_SYNCED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_START_SYNC_NANOS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_STD_DEV_ENTRIES_PER_LEAF;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_SYNC_DURATION_NANOS;
import static com.hazelcast.internal.metrics.ProbeUnit.NS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Merkle tree specific implementation of the {@link WanSyncStats} interface.
 */
public class WanMerkleTreeSyncStats implements WanSyncStats {
    private final UUID uuid;
    @Probe(name = WAN_METRIC_MERKLE_SYNC_START_SYNC_NANOS, unit = NS)
    private final long syncStartNanos = System.nanoTime();

    @Probe(name = WAN_METRIC_MERKLE_SYNC_PARTITIONS_TO_SYNC)
    private final int partitionsToSync;
    @Probe(name = WAN_METRIC_MERKLE_SYNC_PARTITIONS_SYNCED)
    private final AtomicInteger partitionsSynced = new AtomicInteger();
    @Probe(name = WAN_METRIC_MERKLE_SYNC_RECORDS_SYNCED)
    private final AtomicInteger recordsSynced = new AtomicInteger();
    @Probe(name = WAN_METRIC_MERKLE_SYNC_NODES_SYNCED)
    private final AtomicInteger nodesSynced = new AtomicInteger();
    private final AtomicLong sumEntryCountSquares = new AtomicLong();

    @Probe(name = WAN_METRIC_MERKLE_SYNC_SYNC_DURATION_NANOS, unit = NS)
    private volatile long syncDurationNanos;
    @Probe(name = WAN_METRIC_MERKLE_SYNC_MIN_LEAF_ENTRY_COUNT)
    private volatile int minLeafEntryCount = Integer.MAX_VALUE;
    @Probe(name = WAN_METRIC_MERKLE_SYNC_MAX_LEAF_ENTRY_COUNT)
    private volatile int maxLeafEntryCount = Integer.MIN_VALUE;
    @Probe(name = WAN_METRIC_MERKLE_SYNC_AVG_ENTRIES_PER_LEAF)
    private volatile double avgEntriesPerLeaf;
    @Probe(name = WAN_METRIC_MERKLE_SYNC_STD_DEV_ENTRIES_PER_LEAF)
    private volatile double stdDevEntriesPerLeaf;

    WanMerkleTreeSyncStats(UUID uuid, int partitionsToSync) {
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
