package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.wan.impl.WanSyncStats;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_FULL_SYNC_PARTITIONS_SYNCED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_FULL_SYNC_PARTITIONS_TO_SYNC;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_FULL_SYNC_RECORDS_SYNCED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_FULL_SYNC_START_SYNC_NANOS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_FULL_SYNC_SYNC_DURATION_NANOS;
import static com.hazelcast.internal.metrics.ProbeUnit.NS;

/**
 * Full sync specific implementation of the {@link WanSyncStats} interface
 */
public class FullWanSyncStats implements WanSyncStats {
    private final UUID uuid;
    @Probe(name = WAN_METRIC_FULL_SYNC_START_SYNC_NANOS, unit = NS)
    private final long syncStartNanos = System.nanoTime();
    @Probe(name = WAN_METRIC_FULL_SYNC_PARTITIONS_TO_SYNC)
    private final int partitionsToSync;

    @Probe(name = WAN_METRIC_FULL_SYNC_PARTITIONS_SYNCED)
    private AtomicInteger partitionsSynced = new AtomicInteger();
    @Probe(name = WAN_METRIC_FULL_SYNC_RECORDS_SYNCED)
    private AtomicInteger recordsSynced = new AtomicInteger();
    @Probe(name = WAN_METRIC_FULL_SYNC_SYNC_DURATION_NANOS, unit = NS)
    private volatile long syncDurationNanos;

    FullWanSyncStats(UUID uuid, int partitionsToSync) {
        this.uuid = uuid;
        this.partitionsToSync = partitionsToSync;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public int getPartitionsToSync() {
        return partitionsToSync;
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
     * Callback for synchronizing a record.
     */
    void onSyncRecord() {
        this.recordsSynced.incrementAndGet();
    }

    /**
     * Callback for completing synchronization
     */
    void onSyncComplete() {
        syncDurationNanos = System.nanoTime() - syncStartNanos;
    }

    @Override
    public long getDurationSecs() {
        return TimeUnit.NANOSECONDS.toSeconds(syncDurationNanos);
    }

    @Override
    public int getPartitionsSynced() {
        return partitionsSynced.get();
    }

    @Override
    public int getRecordsSynced() {
        return recordsSynced.get();
    }

    @Override
    public String toString() {
        return "FullWanSyncStats{"
                + "uuid=" + uuid
                + ", syncStartNanos=" + syncStartNanos
                + ", partitionsToSync=" + partitionsToSync
                + ", partitionsSynced=" + partitionsSynced
                + ", recordsSynced=" + recordsSynced
                + ", syncDurationNanos=" + syncDurationNanos
                + '}';
    }
}
