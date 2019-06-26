package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.WanReplicationEvent;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Map;

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
    public boolean publishCacheWanEvent(String nameWithPrefix, int partitionId, WanReplicationEvent replicationEvent) {
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
}
