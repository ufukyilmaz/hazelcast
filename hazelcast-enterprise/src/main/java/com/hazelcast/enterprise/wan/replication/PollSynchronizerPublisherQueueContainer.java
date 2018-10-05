package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is an extension to {@link PublisherQueueContainer} that
 * offers synchronization between polling the WAN event queues and other
 * processes such as migration via per-partition locks.
 *
 * For the time a polling a partition's WAN events is blocked, the
 * {@link #pollRandomWanEvent} returns null instead of blocking the poller
 * thread to enable that the poller thread may continue with other not
 * blocked partitions.
 */
public class PollSynchronizerPublisherQueueContainer extends PublisherQueueContainer {
    /**
     * A poll lock is maintained for each partition to ensure that
     * polling the given partition's WAN queue and other processes don't
     * overlap since it may corrupt the internal state of the WAN
     * replication related objects
     */
    private final ConcurrentMap<Integer, ReentrantLock> partitionPollLocksMap = new ConcurrentHashMap<Integer, ReentrantLock>();
    private final ConstructorFunction<Integer, ReentrantLock> pollLockConstructorFunction =
            new ConstructorFunction<Integer, ReentrantLock>() {
        @Override
        public ReentrantLock createNew(Integer partitionId) {
            return new ReentrantLock();
        }
    };

    private final ILogger logger;

    PollSynchronizerPublisherQueueContainer(Node node) {
        super(node);
        logger = node.getLogger(PollSynchronizerPublisherQueueContainer.class);
    }

    @Override
    public WanReplicationEvent pollRandomWanEvent(int partitionId) {
        Lock partitionPollLock = getPartitionPollLock(partitionId);
        if (partitionPollLock.tryLock()) {
            try {
                return super.pollRandomWanEvent(partitionId);
            } finally {
                partitionPollLock.unlock();
            }
        }

        return null;
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
        return ConcurrencyUtil.getOrPutIfAbsent(partitionPollLocksMap, partitionId, pollLockConstructorFunction);
    }
}
