package com.hazelcast.wan;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.instance.impl.Node;
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
    public int removeWanEvents() {
        return 0;
    }

    @Override
    public void collectReplicationData(String wanReplicationName, PartitionReplicationEvent event,
                                       Collection<ServiceNamespace> namespaces, EWRMigrationContainer migrationDataContainer) {
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
