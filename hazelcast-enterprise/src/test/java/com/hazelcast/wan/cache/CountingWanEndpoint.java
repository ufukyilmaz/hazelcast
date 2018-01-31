package com.hazelcast.wan.cache;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WAN endpoint implementation that only counts the number of published events (excluding republished)
 */
public class CountingWanEndpoint implements WanReplicationEndpoint {

    public AtomicLong counter = new AtomicLong();
    private Node node;

    public CountingWanEndpoint() {
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
    public void resume() {

    }

    @Override
    public LocalWanPublisherStats getStats() {
        return null;
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        counter.incrementAndGet();
    }

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
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
    public void collectReplicationData(String wanReplicationName, PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces, EWRMigrationContainer migrationDataContainer) {

    }

    @Override
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {

    }
}