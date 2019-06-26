package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.wan.WanSyncStats;

import java.util.concurrent.TimeUnit;

/**
 * Full sync specific implementation of the {@link WanSyncStats} interface
 */
public class FullWanSyncStats implements WanSyncStats {
    private final long syncStartNanos = System.nanoTime();

    private int partitionsSynced;
    private int recordsSynced;
    private long syncDurationNanos;

    /**
     * Callback for synchronizing a partition
     */
    public void onSyncPartition() {
        partitionsSynced++;
    }

    /**
     * Callback for synchronizing records
     *
     * @param recordsSynced the number of the records synchronized
     */
    public void onSyncRecords(int recordsSynced) {
        this.recordsSynced += recordsSynced;
    }

    /**
     * Callback for completing synchronization
     */
    public void onSyncComplete() {
        syncDurationNanos = System.nanoTime() - syncStartNanos;
    }

    @Override
    public long getDurationSecs() {
        return TimeUnit.NANOSECONDS.toSeconds(syncDurationNanos);
    }

    @Override
    public int getPartitionsSynced() {
        return partitionsSynced;
    }

    @Override
    public int getRecordsSynced() {
        return recordsSynced;
    }
}
