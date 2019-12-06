package com.hazelcast.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * WAN publisher implementation that only counts the number of published events (excluding republished).
 */
public class CountingWanPublisher implements WanReplicationPublisher, HazelcastInstanceAware {

    private AtomicLong counter = new AtomicLong();
    private AtomicLong backupCounter = new AtomicLong();

    private Node node;

    public CountingWanPublisher() {
    }

    public long getCount() {
        return counter.get();
    }

    public long getBackupCount() {
        return counter.get();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.node = ((HazelcastInstanceImpl) hazelcastInstance).node;
    }

    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig config) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void publishReplicationEvent(WanReplicationEvent event) {
        if (isOwnedPartition(((InternalWanReplicationEvent) event).getKey())) {
            counter.incrementAndGet();
        }
    }

    @Override
    public void publishReplicationEventBackup(WanReplicationEvent event) {
        if (!isOwnedPartition(((InternalWanReplicationEvent) event).getKey())) {
            backupCounter.incrementAndGet();
        }
    }

    private boolean isOwnedPartition(Data dataKey) {
        InternalPartitionServiceImpl partitionService = node.partitionService;
        int partitionId = partitionService.getPartitionId(dataKey);
        return partitionService.getPartition(partitionId, false).isLocal();
    }

    @Override
    public void doPrepublicationChecks() {
    }
}
