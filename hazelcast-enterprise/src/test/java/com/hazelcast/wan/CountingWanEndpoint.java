package com.hazelcast.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanSyncEvent;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.PartitionReplicationEvent;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WAN endpoint implementation that only counts the number of published events (excluding republished).
 */
public class CountingWanEndpoint implements WanReplicationEndpoint<Object> {

    private AtomicLong counter = new AtomicLong();
    private AtomicLong backupCounter = new AtomicLong();

    private Node node;

    public CountingWanEndpoint() {
    }

    public long getCount() {
        return counter.get();
    }

    public long getBackupCount() {
        return counter.get();
    }

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig config) {
        this.node = node;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void putBackup(WanReplicationEvent wanReplicationEvent) {
    }

    @Override
    public void pause() {
    }

    @Override
    public void stop() {

    }

    @Override
    public void resume() {
    }

    @Override
    public LocalWanPublisherStats getStats() {
        return null;
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        if (isOwnedPartition(eventObject.getKey())) {
            counter.incrementAndGet();
        }
    }

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        if (!isOwnedPartition(eventObject.getKey())) {
            backupCounter.incrementAndGet();
        }
    }

    private boolean isOwnedPartition(Data dataKey) {
        InternalPartitionServiceImpl partitionService = node.partitionService;
        int partitionId = partitionService.getPartitionId(dataKey);
        return partitionService.getPartition(partitionId, false).isLocal();
    }

    @Override
    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
    }

    @Override
    public void checkWanReplicationQueues() {
    }

    @Override
    public void publishSyncEvent(WanSyncEvent syncRequest) {
    }

    @Override
    public Object prepareEventContainerReplicationData(
            PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        return null;
    }

    @Override
    public void processEventContainerReplicationData(int partitionId, Object eventContainer) {

    }

    @Override
    public int removeWanEvents() {
        return 0;
    }

    @Override
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
    }

    @Override
    public int removeWanEvents(int partitionId, String serviceName) {
        return 0;
    }

    @Override
    public int removeWanEvents(int partitionId, String serviceName, String objectName, int count) {
        return 0;
    }
}
