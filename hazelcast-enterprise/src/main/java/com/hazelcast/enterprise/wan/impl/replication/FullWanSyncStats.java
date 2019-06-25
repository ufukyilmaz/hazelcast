package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.wan.WanSyncStats;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Full sync specific implementation of the {@link WanSyncStats} interface
 */
public class FullWanSyncStats implements WanSyncStats {
    private final UUID uuid;
    private final long syncStartNanos = System.nanoTime();
    private final int partitionsToSync;

    private AtomicInteger partitionsSynced = new AtomicInteger();
    private AtomicInteger recordsSynced = new AtomicInteger();
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
