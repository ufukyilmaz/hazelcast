package com.hazelcast.enterprise.wan;

/**
 * Interface for WAN queue migration related events. Can be implemented
 * by WAN publishers to listen to WAN queue migration events, for example
 * to maintain the WAN event counters.
 * <p>
 * None of the methods of this interface is expected to block or fail.
 *
 * @see com.hazelcast.spi.partition.PartitionMigrationEvent
 * @see com.hazelcast.spi.partition.MigrationAwareService
 */
public interface WanEventQueueMigrationListener {
    /**
     * Indicates that migration started for a given partition
     *
     * @param partitionId         the partition being migrated
     * @param currentReplicaIndex the current replica index of the partition
     * @param newReplicaIndex     the new replica index if the partition
     */
    void onMigrationStart(int partitionId, int currentReplicaIndex, int newReplicaIndex);

    /**
     * Indicates that migration is committing for a given partition
     *
     * @param partitionId         the partition being migrated
     * @param currentReplicaIndex the current replica index of the partition
     * @param newReplicaIndex     the new replica index if the partition
     */
    void onMigrationCommit(int partitionId, int currentReplicaIndex, int newReplicaIndex);

    /**
     * Indicates that migration is rolling back for a given partition
     *
     * @param partitionId         the partition being migrated
     * @param currentReplicaIndex the current replica index of the partition
     * @param newReplicaIndex     the new replica index if the partition
     */
    void onMigrationRollback(int partitionId, int currentReplicaIndex, int newReplicaIndex);

    /**
     * Indicates that a partition's WAN event queue is cleared during
     * migration
     *
     * @param partitionId           the partition being migrated
     * @param currentReplicaIndex   the current replica index of the partition
     * @param clearedQueueDepth     the depth of the migrated WAN queue
     */
    void onWanQueueClearedDuringMigration(int partitionId, int currentReplicaIndex, int clearedQueueDepth);
}
