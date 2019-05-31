package com.hazelcast.wan;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
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
public class CountingWanEndpoint implements WanReplicationEndpoint {

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
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
        this.node = node;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void removeBackup(WanReplicationEvent wanReplicationEvent) {
    }

    @Override
    public void putBackup(WanReplicationEvent wanReplicationEvent) {
    }

    @Override
    public PublisherQueueContainer getPublisherQueueContainer() {
        return new PublisherQueueContainer(node);
    }

    @Override
    public void addMapQueue(String key, int partitionId, WanReplicationEventQueue value) {
    }

    @Override
    public void addCacheQueue(String key, int partitionId, WanReplicationEventQueue value) {
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
    public void clearQueues() {
    }

    @Override
    public void collectReplicationData(String wanReplicationName, PartitionReplicationEvent event,
                                       Collection<ServiceNamespace> namespaces, EWRMigrationContainer migrationDataContainer) {
    }

    @Override
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
    }
}
