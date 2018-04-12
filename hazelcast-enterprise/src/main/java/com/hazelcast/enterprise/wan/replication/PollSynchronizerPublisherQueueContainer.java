package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.instance.Node;
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
    private final ConcurrentMap<Integer, Lock> partitionPollLocksMap = new ConcurrentHashMap<Integer, Lock>();
    private final ConstructorFunction<Integer, Lock> pollLockConstructorFunction = new ConstructorFunction<Integer, Lock>() {
        @Override
        public Lock createNew(Integer partitionId) {
            return new ReentrantLock();
        }
    };

    PollSynchronizerPublisherQueueContainer(Node node) {
        super(node);
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
        Lock pollLock = getPartitionPollLock(partitionId);
        pollLock.unlock();
    }

    private Lock getPartitionPollLock(int partitionId) {
        return ConcurrencyUtil.getOrPutIfAbsent(partitionPollLocksMap, partitionId, pollLockConstructorFunction);
    }
}
