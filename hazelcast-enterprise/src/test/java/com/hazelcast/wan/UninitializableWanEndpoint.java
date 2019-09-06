package com.hazelcast.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanSyncEvent;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.partition.PartitionReplicationEvent;

import java.util.Collection;
import java.util.Set;

/**
 * WAN endpoint implementation that throws an exception on initialization.
 */
public class UninitializableWanEndpoint implements WanReplicationEndpoint<Object> {

    public UninitializableWanEndpoint() {
    }

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig pc) {
        throw new UnsupportedOperationException("This endpoint cannot be initialized!");
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void putBackup(WanReplicationEvent event) {
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
    public void publishReplicationEvent(WanReplicationEvent event) {
    }

    @Override
    public void publishReplicationEventBackup(WanReplicationEvent event) {
    }

    @Override
    public void republishReplicationEvent(WanReplicationEvent event) {
    }

    @Override
    public void checkWanReplicationQueues() {
    }

    @Override
    public void publishSyncEvent(WanSyncEvent syncRequest) {
    }

    @Override
    public Object prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                       Collection<ServiceNamespace> namespaces) {
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