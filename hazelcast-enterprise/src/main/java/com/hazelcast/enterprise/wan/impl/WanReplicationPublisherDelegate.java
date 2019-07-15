package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Delegating WAN replication publisher implementation. This implementation
 * is a container for multiple WAN publisher endpoints.
 * When publishing an event on this delegate, all endpoints are notified.
 */
public final class WanReplicationPublisherDelegate implements WanReplicationPublisher {
    /** Non-null WAN replication name */
    final String name;
    /** Non-null WAN publisher endpoints, grouped by publisher ID */
    final ConcurrentMap<String, WanReplicationEndpoint> endpoints;

    public WanReplicationPublisherDelegate(String name,
                                           ConcurrentMap<String, WanReplicationEndpoint> endpoints) {
        checkNotNull(name, "WAN publisher name should not be null");
        checkNotNull(endpoints, "WAN publisher endpoint map should not be null");
        this.name = name;
        this.endpoints = endpoints;
    }

    /** Returns all {@link WanReplicationEndpoint}s for this delegate */
    public Collection<WanReplicationEndpoint> getEndpoints() {
        return endpoints.values();
    }

    /**
     * Returns the {@link WanReplicationEndpoint} with the {@code publisherId}
     * or {@code null} if it doesn't exist.
     */
    public WanReplicationEndpoint getEndpoint(String publisherId) {
        return endpoints.get(publisherId);
    }

    public void addEndpoint(String publisherId, WanReplicationEndpoint endpoint) {
        endpoints.put(publisherId, endpoint);
    }

    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     * Publishes a replication event to all endpoints to which this publisher
     * delegates.
     */
    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.publishReplicationEvent(serviceName, eventObject);
        }
    }

    /**
     * {@inheritDoc}
     * Publishes a backup replication event to all endpoints to which this
     * publisher delegates.
     */
    @Override
    public void publishReplicationEventBackup(String serviceName, ReplicationEventObject eventObject) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.publishReplicationEventBackup(serviceName, eventObject);
        }
    }

    /**
     * {@inheritDoc}
     * Publishes a replication event to all endpoints to which this publisher
     * delegates.
     */
    @Override
    public void publishReplicationEvent(WanReplicationEvent wanReplicationEvent) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.publishReplicationEvent(wanReplicationEvent);
        }
    }

    public Map<String, LocalWanPublisherStats> getStats() {
        final Map<String, LocalWanPublisherStats> statsMap = createHashMap(endpoints.size());
        for (Map.Entry<String, WanReplicationEndpoint> endpointEntry : endpoints.entrySet()) {
            final String endpointName = endpointEntry.getKey();
            final WanReplicationEndpoint endpoint = endpointEntry.getValue();
            statsMap.put(endpointName, endpoint.getStats());
        }
        return statsMap;
    }

    @Override
    public void checkWanReplicationQueues() {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.checkWanReplicationQueues();
        }
    }

    /**
     * Collect all replication data matching the replication event and collection
     * of namespaces being replicated.
     * Returns containers for WAN replication events grouped by WAN publisher ID.
     *
     * @param event      the replication event
     * @param namespaces the object namespaces which are being replicated
     * @return a map from WAN publisher ID to container object for WAN replication events
     */
    public Map<String, Object> prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                                    Collection<ServiceNamespace> namespaces) {
        Map<String, Object> eventContainers = createHashMap(endpoints.size());
        for (Entry<String, WanReplicationEndpoint> endpointEntry : endpoints.entrySet()) {
            Object eventContainer = endpointEntry.getValue()
                                                 .prepareEventContainerReplicationData(event, namespaces);
            if (eventContainer != null) {
                String publisherId = endpointEntry.getKey();
                eventContainers.put(publisherId, eventContainer);
            }
        }
        return eventContainers;
    }

    /**
     * Collect the namespaces of all queues that should be replicated by the
     * replication event.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            endpoint.collectAllServiceNamespaces(event, namespaces);
        }
    }

    /**
     * Releases all resources for the map with the given {@code mapName}.
     *
     * @param mapName the map mapName
     */
    public void destroyMapData(String mapName) {
        for (WanReplicationEndpoint endpoint : endpoints.values()) {
            if (endpoint instanceof WanBatchReplication) {
                ((WanBatchReplication) endpoint).destroyMapData(mapName);
            }
        }
    }
}
