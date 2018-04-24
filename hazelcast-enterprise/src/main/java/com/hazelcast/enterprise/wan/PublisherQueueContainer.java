package com.hazelcast.enterprise.wan;

import com.hazelcast.instance.Node;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WAN event queue container for WAN replication publishers. Each WAN queue
 * container is responsible for a specific partition. Provides methods to
 * push/pull WAN events to/from queues.
 */
public class PublisherQueueContainer {
    /**
     * Partition ID to event container map
     */
    private Map<Integer, PartitionWanEventContainer> publisherEventQueueMap
            = new ConcurrentHashMap<Integer, PartitionWanEventContainer>();

    public PublisherQueueContainer(Node node) {
        for (IPartition partition : node.getPartitionService().getPartitions()) {
            publisherEventQueueMap.put(partition.getPartitionId(), new PartitionWanEventContainer());
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
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.pollCacheWanEvent(nameWithPrefix);
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
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.publishCacheWanEvent(nameWithPrefix, replicationEvent);
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
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.pollMapWanEvent(mapName);
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
        PartitionWanEventContainer wanEventContainer = publisherEventQueueMap.get(partitionId);
        return wanEventContainer.publishMapWanEvent(mapName, replicationEvent);
    }

    /**
     * Returns a random replication event for the {@code partitionId}.
     *
     * @param partitionId the partition ID for the replication event
     * @return a random replication event for the given partition ID
     */
    public WanReplicationEvent pollRandomWanEvent(int partitionId) {
        return publisherEventQueueMap.get(partitionId).pollRandomWanEvent();
    }

    /**
     * Returns the map of partition ID to {@link PartitionWanEventContainer}
     * for the specific partition
     */
    public Map<Integer, PartitionWanEventContainer> getPublisherEventQueueMap() {
        return publisherEventQueueMap;
    }

    /**
     * Returns the {@link PartitionWanEventContainer} for the specified
     * {@code partitionId}.
     *
     * @param partitionId the partition ID for the WAN event container
     * @return the WAN event container
     */
    public PartitionWanEventContainer getPublisherEventQueue(int partitionId) {
        return publisherEventQueueMap.get(partitionId);
    }

    public void clearQueues() {
        for (PartitionWanEventContainer partitionWanEventContainer : publisherEventQueueMap.values()) {
            partitionWanEventContainer.clear();
        }
    }

    /**
     * Returns the size of all WAN queues for the given {@code partitionId}.
     * @param partitionId the partition ID
     * @return the size of the WAN queue
     */
    public int size(int partitionId) {
        return publisherEventQueueMap.get(partitionId).size();
    }
}
