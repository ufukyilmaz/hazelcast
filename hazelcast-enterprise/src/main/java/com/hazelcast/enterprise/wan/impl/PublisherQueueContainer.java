package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.WanReplicationEvent;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * WAN event queue container for WAN replication publishers. Each WAN queue
 * container is responsible for a specific partition. Provides methods to
 * push/pull WAN events to/from queues.
 */
public class PublisherQueueContainer {
    private final PartitionWanEventContainer[] containers;

    public PublisherQueueContainer(Node node) {
        int partitionCount = node.getPartitionService().getPartitionCount();

        containers = new PartitionWanEventContainer[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            containers[partitionId] = new PartitionWanEventContainer();
        }
    }

    /**
     * Polls the wan event queue for the cache with the name
     * {@code nameWithPrefix} on partition {@code partitionId}.
     *
     * @param nameWithPrefix the cache name
     * @param partitionId    the partition of the wan event
     * @return the wan replication event
     */
    public WanReplicationEvent pollCacheWanEvent(String nameWithPrefix, int partitionId) {
        return getEventQueue(partitionId)
                .pollCacheWanEvent(nameWithPrefix);
    }

    /**
     * Publishes the {@code replicationEvent} for the cache with the name
     * {@code nameWithPrefix} on the partition {@code partitionId}.
     *
     * @param nameWithPrefix   the cache name
     * @param partitionId      the partition ID for the published event
     * @param replicationEvent the published replication event
     * @return {@code true} if the element was added to this queue, else {@code false}
     */
    public boolean publishCacheWanEvent(String nameWithPrefix,
                                        int partitionId,
                                        WanReplicationEvent replicationEvent) {
        return getEventQueue(partitionId)
                .publishCacheWanEvent(nameWithPrefix, replicationEvent);
    }

    /**
     * Polls the wan event queue for the map with the name {@code mapName} on
     * partition {@code partitionId}.
     *
     * @param mapName     the map name
     * @param partitionId the partition of the wan event
     * @return the wan replication event
     */
    public WanReplicationEvent pollMapWanEvent(String mapName, int partitionId) {
        return getEventQueue(partitionId)
                .pollMapWanEvent(mapName);
    }

    /**
     * Publishes the {@code replicationEvent} for the given {@code mapName}
     * map on the partition {@code partitionId}
     *
     * @param mapName          the name of the map for which the event is
     *                         published
     * @param partitionId      the partition ID for the published event
     * @param replicationEvent the published replication event
     * @return {@code true} if the element was added to this queue, else
     * {@code false}
     */
    public boolean publishMapWanEvent(String mapName, int partitionId, WanReplicationEvent replicationEvent) {
        return getEventQueue(partitionId)
                .publishMapWanEvent(mapName, replicationEvent);
    }

    /**
     * Removes at most the given number of available elements from a random WAN
     * queue and for the given partition and adds them to the given collection.
     *
     * @param partitionId     the partition ID for which a random WAN queue should be drained
     * @param drainTo         the collection to which to drain events to
     * @param elementsToDrain the maximum number of events to drain
     */
    public void drainRandomWanQueue(int partitionId,
                                    Collection<WanReplicationEvent> drainTo,
                                    int elementsToDrain) {
        getEventQueue(partitionId)
                .drainRandomWanQueue(drainTo, elementsToDrain);
    }

    /**
     * Returns the {@link PartitionWanEventContainer} for the specified
     * {@code partitionId}.
     *
     * @param partitionId the partition ID for the WAN event container
     * @return the WAN event container
     */
    public PartitionWanEventContainer getEventQueue(int partitionId) {
        return containers[partitionId];
    }

    /**
     * Returns the size of all WAN queues for the given {@code partitionId}.
     *
     * @param partitionId the partition ID
     * @return the size of the WAN queue
     */
    public int size(int partitionId) {
        return getEventQueue(partitionId).size();
    }

    /**
     * Drains all the queues stored in this container and returns the
     * total of the drained elements in a map, per partition.
     *
     * @return the number of drained elements per partition
     */
    public Map<Integer, Integer> drainQueues() {
        Map<Integer, Integer> partitionDrainsMap = MapUtil.createHashMap(containers.length);
        for (int partitionId = 0; partitionId < containers.length; partitionId++) {
            PartitionWanEventContainer partitionWanEventContainer = getEventQueue(partitionId);
            int drained = partitionWanEventContainer.drain();
            partitionDrainsMap.put(partitionId, drained);
        }

        return partitionDrainsMap;
    }

    /**
     * Drains all the queues maintained for the given partition.
     *
     * @param partitionId the partition ID for which queues need to be drained
     * @return the number of drained elements
     */
    public int drainQueues(int partitionId) {
        return getEventQueue(partitionId).drain();
    }

    /**
     * Drains all the map queues maintained for the given partition.
     *
     * @param partitionId the partition ID for which queues need to be drained
     * @return the number of drained elements
     */
    public int drainMapQueues(int partitionId) {
        return getEventQueue(partitionId).drainMap();
    }

    /**
     * Drains all the cache queues maintained for the given partition.
     *
     * @param partitionId the partition ID for which queues need to be drained
     * @return the number of drained elements
     */
    public int drainCacheQueues(int partitionId) {
        return getEventQueue(partitionId).drainCache();
    }

    /**
     * Returns all of the partition WAN event queue containers.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public PartitionWanEventContainer[] getContainers() {
        return containers;
    }

    /**
     * Clears all WAN queues in all containers.
     */
    public void clear() {
        for (PartitionWanEventContainer container : containers) {
            container.clear();
        }
    }

    /**
     * Returns a container containing the WAN events for the given replication
     * {@code event} and {@code namespaces} to be replicated. The replication
     * here refers to the intra-cluster replication between members in a single
     * cluster and does not refer to WAN replication, e.g. between two clusters.
     *
     * @param event      the replication event
     * @param namespaces namespaces which will be replicated
     * @return the WAN event container
     */
    public WanEventMigrationContainer prepareEventContainerReplicationData(
            PartitionReplicationEvent event,
            Collection<ServiceNamespace> namespaces) {
        final PartitionWanEventContainer partitionContainer = getEventQueue(event.getPartitionId());
        if (partitionContainer != null) {
            final PartitionWanEventQueueMap mapQueues = collectNamespaces(
                    partitionContainer.getMapEventQueueMapByBackupCount(event.getReplicaIndex()),
                    MapService.SERVICE_NAME, namespaces);

            final PartitionWanEventQueueMap cacheQueues = collectNamespaces(
                    partitionContainer.getCacheEventQueueMapByBackupCount(event.getReplicaIndex()),
                    CacheService.SERVICE_NAME, namespaces);
            return new WanEventMigrationContainer(mapQueues, cacheQueues);
        }

        return null;
    }

    /**
     * Collect the namespaces of all WAN event containers that should be replicated
     * by the replication event.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
        final int partitionId = event.getPartitionId();
        final PartitionWanEventContainer partitionContainer = getEventQueue(partitionId);

        if (partitionContainer == null) {
            return;
        }
        final int replicaIndex = event.getReplicaIndex();
        final PartitionWanEventQueueMap mapQueues = partitionContainer.getMapEventQueueMapByBackupCount(replicaIndex);
        final PartitionWanEventQueueMap cacheQueues = partitionContainer.getCacheEventQueueMapByBackupCount(replicaIndex);
        for (String mapName : mapQueues.keySet()) {
            namespaces.add(MapService.getObjectNamespace(mapName));
        }
        for (String cacheName : cacheQueues.keySet()) {
            namespaces.add(CacheService.getObjectNamespace(cacheName));
        }
    }


    /**
     * Filter and collect {@link WanReplicationEventQueue}s that contain events matching the
     * {@code serviceName} and one of the provided {@code namespaces}. The namespaces must be
     * of the type {@link ObjectNamespace}.
     *
     * @param queues      WAN event queue map of a partition and a specific service (map/cache)
     * @param serviceName the service name for which we are collecting WAN queues
     * @param namespaces  the collection of {@link ObjectNamespace}s for which we are collecting WAN queues
     * @return the filtered map from distributed object name to WAN queue
     */
    private PartitionWanEventQueueMap collectNamespaces(PartitionWanEventQueueMap queues, String serviceName,
                                                        Collection<ServiceNamespace> namespaces) {
        if (queues.isEmpty()) {
            return null;
        }

        final PartitionWanEventQueueMap filteredQueues = new PartitionWanEventQueueMap();
        for (ServiceNamespace namespace : namespaces) {
            if (!serviceName.equals(namespace.getServiceName())) {
                continue;
            }

            final ObjectNamespace ns = (ObjectNamespace) namespace;
            final WanReplicationEventQueue q = queues.get(ns.getObjectName());
            if (q != null) {
                filteredQueues.put(ns.getObjectName(), q);
            }
        }
        return filteredQueues;
    }
}
