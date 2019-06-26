package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.PublisherQueueContainer;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is an extension to {@link PublisherQueueContainer} that
 * offers synchronization between polling the WAN event queues and other
 * processes such as migration via per-partition locks.
 * <p>
 * For the time a polling a partition's WAN events is blocked, the
 * {@link #drainRandomWanQueue(int, Collection, int)} will not drain any
 * events.
 */
public class PollSynchronizerPublisherQueueContainer extends PublisherQueueContainer {
    /**
     * A poll lock is maintained for each partition to ensure that
     * polling the given partition's WAN queue and other processes don't
     * overlap since it may corrupt the internal state of the WAN
     * replication related objects
     */
    private final AtomicReferenceArray<ReentrantLock> partitionPollLocks;

    private final ILogger logger;

    PollSynchronizerPublisherQueueContainer(Node node) {
        super(node);
        int partitionCount = node.getNodeEngine().getPartitionService().getPartitionCount();
        partitionPollLocks = new AtomicReferenceArray<ReentrantLock>(partitionCount);
        logger = node.getLogger(PollSynchronizerPublisherQueueContainer.class);
    }

    @Override
    public void drainRandomWanQueue(int partitionId, Collection<WanReplicationEvent> drainTo, int elementsToDrain) {
        Lock partitionPollLock = getPartitionPollLock(partitionId);
        if (partitionPollLock.tryLock()) {
            try {
                super.drainRandomWanQueue(partitionId, drainTo, elementsToDrain);
            } finally {
                partitionPollLock.unlock();
            }
        }
    }

    /**
     * Blocks polling the WAN event queues for a partition
     *
     * @param partitionId The partition which to be blocked for polling
     * @see #unblockPollingPartition
     */
    void blockPollingPartition(int partitionId) {
        Lock pollLock = getPartitionPollLock(partitionId);
        pollLock.lock();
    }

    /**
     * Unblocks polling the WAN event queues for a partition
     *
     * @param partitionId The partition which to be blocked for polling
     * @see #unblockPollingPartition
     */
    void unblockPollingPartition(int partitionId) {
        ReentrantLock pollLock = getPartitionPollLock(partitionId);

        if (pollLock.isLocked()) {
            // there is a race between migration and creating WAN endpoints
            // creating a WAN endpoint between beforeMigration and finalizeMigration
            // may leave a particular pollLock unlocked for which an unlock
            // attempt would fail with IllegalMonitorStateException
            // documented in details in
            // https://github.com/hazelcast/hazelcast-enterprise/issues/2442
            pollLock.unlock();
        }
    }

    private ReentrantLock getPartitionPollLock(int partitionId) {
        ReentrantLock lock = partitionPollLocks.get(partitionId);
        if (lock != null) {
            return lock;
        }
        partitionPollLocks.compareAndSet(partitionId, null, new ReentrantLock());
        return partitionPollLocks.get(partitionId);
    }
}
