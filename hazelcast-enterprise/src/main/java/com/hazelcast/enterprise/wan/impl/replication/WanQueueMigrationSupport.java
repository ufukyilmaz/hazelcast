package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.WanReplicationEventQueue;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.wan.WanReplicationPublisherMigrationListener;

import java.util.function.Predicate;

/**
 * Class responsible for processing migration events and guaranteeing the
 * correctness of the internal state of the replication implementation
 * objects
 */
class WanQueueMigrationSupport implements WanReplicationPublisherMigrationListener {
    private final PollSynchronizerPublisherQueueContainer eventQueueContainer;
    private final WanElementCounter wanCounter;

    WanQueueMigrationSupport(PollSynchronizerPublisherQueueContainer eventQueueContainer,
                             WanElementCounter wanCounter) {
        this.eventQueueContainer = eventQueueContainer;
        this.wanCounter = wanCounter;
    }

    @Override
    public void onMigrationStart(PartitionMigrationEvent event) {
        eventQueueContainer.blockPollingPartition(event.getPartitionId());
    }

    @Override
    public void onMigrationCommit(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            removeWanQueues(event.getPartitionId(), event.getCurrentReplicaIndex(), event.getNewReplicaIndex());
        }

        int partitionId = event.getPartitionId();
        int newReplicaIndex = event.getNewReplicaIndex();
        int currentReplicaIndex = event.getCurrentReplicaIndex();
        if (newReplicaIndex == 0) {
            // this member was either a backup replica or not a replica at all
            // now it is a primary replica
            int qSize = eventQueueContainer.getEventQueue(partitionId).size();
            wanCounter.moveFromBackupToPrimaryCounter(qSize);
        } else if (currentReplicaIndex == 0 && newReplicaIndex > 0) {
            // this member was a primary replica, now it is a backup replica
            int qSize = eventQueueContainer.getEventQueue(partitionId).size();
            wanCounter.moveFromPrimaryToBackupCounter(qSize);
        }
        eventQueueContainer.unblockPollingPartition(partitionId);
    }

    @Override
    public void onMigrationRollback(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            removeWanQueues(event.getPartitionId(), event.getCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        }
        eventQueueContainer.unblockPollingPartition(event.getPartitionId());
    }

    private void removeWanQueues(int partitionId,
                                 int currentReplicaIndex,
                                 int thresholdReplicaIndex) {
        // queue depth cannot change between invocations of the size() and the drain() methods, since
        // 1) we are on a partition operation thread -> no operations can emit WAN events
        // 2) polling the queue is disabled for the time of the migration
        int sizeBeforeClear = 0;
        Predicate<WanReplicationEventQueue> predicate =
                q -> thresholdReplicaIndex < 0 || q.getBackupCount() < thresholdReplicaIndex;

        sizeBeforeClear += eventQueueContainer.drainMapQueuesMatchingPredicate(partitionId, predicate);
        sizeBeforeClear += eventQueueContainer.drainCacheQueuesMatchingPredicate(partitionId, predicate);

        onWanQueueClearedDuringMigration(currentReplicaIndex, sizeBeforeClear);
    }

    private void onWanQueueClearedDuringMigration(int currentReplicaIndex, int clearedQueueDepth) {
        if (currentReplicaIndex == 0) {
            wanCounter.decrementPrimaryElementCounter(clearedQueueDepth);
        } else {
            wanCounter.decrementBackupElementCounter(clearedQueueDepth);
        }
    }
}
