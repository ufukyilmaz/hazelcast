package com.hazelcast.wan;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.PartitionReplicationEvent;

import java.util.Collection;
import java.util.Set;

/**
 * WAN endpoint implementation that throws an exception on initialization.
 */
public class UninitializableWanEndpoint implements WanReplicationEndpoint {

    public UninitializableWanEndpoint() {
    }

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig wanPublisherConfig) {
        throw new UnsupportedOperationException("This endpoint cannot be initialized!");
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
        return null;
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
    public void collectReplicationData(String wanReplicationName, PartitionReplicationEvent event,
                                       Collection<ServiceNamespace> namespaces, EWRMigrationContainer migrationDataContainer) {
    }

    @Override
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
    }
}
