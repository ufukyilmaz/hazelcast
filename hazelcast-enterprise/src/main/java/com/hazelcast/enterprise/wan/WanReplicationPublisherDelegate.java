package com.hazelcast.enterprise.wan;

import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Map;

/**
 * Delegating WAN replication publisher implementation.
 */
public final class WanReplicationPublisherDelegate
        implements WanReplicationPublisher {

    final String name;
    final Map<String, WanReplicationEndpoint> endpoints;

    public WanReplicationPublisherDelegate(String name, Map<String, WanReplicationEndpoint> endpoints) {
        this.name = name;
        this.endpoints = endpoints;
    }

    public Map<String, WanReplicationEndpoint> getEndpoints() {
        return endpoints;
    }

    public String getName() {
        return name;
    }

    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.publishReplicationEvent(serviceName, eventObject);
        }
    }

    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.publishReplicationEventBackup(serviceName, eventObject);
        }
    }

    @Override
    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.publishReplicationEvent(wanReplicationEvent);
        }
    }

    public Map<String, LocalWanPublisherStats> getStats() {
        Map<String, LocalWanPublisherStats> statsMap = MapUtil.createHashMap(endpoints.size());
        for (Map.Entry<String, WanReplicationEndpoint> endpointEntry : endpoints.entrySet()) {
            String endpointName = endpointEntry.getKey();
            LocalWanPublisherStats wanPublisherStats = endpointEntry.getValue().getStats();
            statsMap.put(endpointName, wanPublisherStats);
        }
        return statsMap;
    }

    public void checkWanReplicationQueues() {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.checkWanReplicationQueues();
        }
    }
}
