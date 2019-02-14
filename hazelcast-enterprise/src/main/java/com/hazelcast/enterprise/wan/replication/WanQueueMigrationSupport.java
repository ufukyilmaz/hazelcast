package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.WanEventQueueMigrationListener;

/**
 * Class responsible for processing migration events and guaranteeing the
 * correctness of the internal state of the replication implementation
 * objects
 */
class WanQueueMigrationSupport implements WanEventQueueMigrationListener {
    private final PollSynchronizerPublisherQueueContainer eventQueueContainer;
    private final WanElementCounter wanCounter;

    WanQueueMigrationSupport(PollSynchronizerPublisherQueueContainer eventQueueContainer,
                             WanElementCounter wanCounter) {
        this.eventQueueContainer = eventQueueContainer;
        this.wanCounter = wanCounter;
    }

    @Override
    public void onMigrationStart(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
        eventQueueContainer.blockPollingPartition(partitionId);
    }

    @Override
    public void onMigrationCommit(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
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
    public void onMigrationRollback(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
        eventQueueContainer.unblockPollingPartition(partitionId);
    }

    @Override
    public void onWanQueueClearedDuringMigration(int partitionId, int currentReplicaIndex, int clearedQueueDepth) {
        if (currentReplicaIndex == 0) {
            wanCounter.decrementPrimaryElementCounter(clearedQueueDepth);
        } else {
            wanCounter.decrementBackupElementCounter(clearedQueueDepth);
        }
    }
}
