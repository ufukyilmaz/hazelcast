package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.internal.util.QueueUtil;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

import java.util.Collection;
import java.util.Map;

/**
 * Contains all map/cache event queues of a partition.
 */
public class PartitionWanEventContainer {

    private final PartitionWanEventQueueMap mapWanEventQueueMap = new PartitionWanEventQueueMap();
    private final PartitionWanEventQueueMap cacheWanEventQueueMap = new PartitionWanEventQueueMap();

    //Below two fields are for random polling
    private PartitionWanEventQueueMap current = mapWanEventQueueMap;
    private PartitionWanEventQueueMap next = cacheWanEventQueueMap;

    /**
     * Publishes the {@code replicationEvent} for the given {@code mapName}
     * map on the partition {@code partitionId}
     *
     * @param mapName the name of the map for which the event is
     *                published
     * @param event   the published replication event
     * @return {@code true} if the element was added to this queue, else
     * {@code false}
     */
    public boolean publishMapWanEvent(String mapName, InternalWanReplicationEvent event) {
        return mapWanEventQueueMap.offerEvent(event, mapName, event.getBackupCount());
    }

    /**
     * Return the head of the wan event queue for the {@code mapName}
     *
     * @param mapName the map for which an event is polled
     * @return the replication event
     */
    public InternalWanReplicationEvent pollMapWanEvent(String mapName) {
        return mapWanEventQueueMap.pollEvent(mapName);
    }

    /**
     * Return the head of the wan event queue for the {@code cacheName}
     *
     * @param cacheName the cache for which an event is polled
     * @return the replication event
     */
    public InternalWanReplicationEvent pollCacheWanEvent(String cacheName) {
        return cacheWanEventQueueMap.pollEvent(cacheName);
    }

    /**
     * Removes at most the given number of available elements from a random WAN
     * queue and adds them to the given collection.
     *
     * @param drainTo         the collection to which to drain events to
     * @param elementsToDrain the maximum number of events to drain
     */
    public void drainRandomWanQueue(Collection<InternalWanReplicationEvent> drainTo,
                                    int elementsToDrain) {
        int drained = drainRandomWanQueue(current, drainTo, elementsToDrain);

        if (drained == 0) {
            PartitionWanEventQueueMap temp = current;
            current = next;
            next = temp;
        } else {
            drainRandomWanQueue(next, drainTo, elementsToDrain);
        }
    }

    public int size() {
        int size = 0;
        for (Map.Entry<String, WanReplicationEventQueue> eventQueueMapEntry : mapWanEventQueueMap.entrySet()) {
            WanReplicationEventQueue eventQueue = eventQueueMapEntry.getValue();
            if (eventQueue != null) {
                size += eventQueue.size();
            }
        }

        for (Map.Entry<String, WanReplicationEventQueue> eventQueueMapEntry : cacheWanEventQueueMap.entrySet()) {
            WanReplicationEventQueue eventQueue = eventQueueMapEntry.getValue();
            if (eventQueue != null) {
                size += eventQueue.size();
            }
        }

        return size;
    }

    /**
     * Removes at most the given number of available elements from a random WAN
     * queue in the {@code eventQueueMap} and adds them to the given collection.
     *
     * @param eventQueueMap   the map containing WAN event queues as values
     * @param drainTo         the collection to which to drain events to
     * @param elementsToDrain the maximum number of events to drain
     * @return the number of elements transferred
     */
    private int drainRandomWanQueue(PartitionWanEventQueueMap eventQueueMap,
                                    Collection<InternalWanReplicationEvent> drainTo,
                                    int elementsToDrain) {
        for (WanReplicationEventQueue eventQueue : eventQueueMap.values()) {
            if (eventQueue != null) {
                int drained = eventQueue.drainTo(drainTo, elementsToDrain);
                if (drained > 0) {
                    return drained;
                }
            }
        }
        return 0;
    }

    public PartitionWanEventQueueMap getMapEventQueueMapByBackupCount(int backupCount) {
        return getEventQueueMapByBackupCount(mapWanEventQueueMap, backupCount);
    }

    public PartitionWanEventQueueMap getCacheEventQueueMapByBackupCount(int backupCount) {
        return getEventQueueMapByBackupCount(cacheWanEventQueueMap, backupCount);
    }

    private PartitionWanEventQueueMap getEventQueueMapByBackupCount(PartitionWanEventQueueMap wanEventQueueMap,
                                                                    int backupCount) {
        PartitionWanEventQueueMap filteredEventQueueMap = new PartitionWanEventQueueMap();
        for (Map.Entry<String, WanReplicationEventQueue> entry : wanEventQueueMap.entrySet()) {
            String name = entry.getKey();
            WanReplicationEventQueue queue = entry.getValue();
            if (queue.getBackupCount() >= backupCount) {
                filteredEventQueueMap.put(name, queue);
            }
        }
        return filteredEventQueueMap;
    }

    /**
     * Publishes the {@code replicationEvent} for the given {@code cacheName}
     * cache on the partition {@code partitionId}
     *
     * @param cacheName the name of the cache for which the event is
     *                  published
     * @param event     the published replication event
     * @return {@code true} if the element was added to this queue, else
     * {@code false}
     */
    public boolean publishCacheWanEvent(String cacheName,
                                        InternalWanReplicationEvent event) {
        return cacheWanEventQueueMap.offerEvent(event, cacheName, event.getBackupCount());
    }

    public void clear() {
        mapWanEventQueueMap.clear();
        cacheWanEventQueueMap.clear();
    }

    /**
     * Drains all the queues maintained for the given partition. It is
     * different from {@code clear} in the way that this method removes
     * elements from all the queues equal to the size of the queue known
     * upfront. This means this method doesn't guarantee that the
     * queues will be empty on return.
     *
     * @return the number of drained elements
     */
    int drain() {
        return drainMap() + drainCache();
    }

    /**
     * Drains all the queues holding map WAN events maintained for the
     * given partition. It is different from {@code clear} in the way
     * that this method removes elements from all the queues equal to
     * the size of the queue known upfront. This means this method
     * doesn't guarantee that the queues will be empty on return.
     *
     * @return the number of drained elements
     */
    int drainMap() {
        return drain(mapWanEventQueueMap);
    }

    /**
     * Drains all the queues holding cache WAN events maintained for the
     * given partition. It is different from {@code clear} in the way
     * that this method removes elements from all the queues equal to
     * the size of the queue known upfront. This means this method
     * doesn't guarantee that the queues will be empty on return.
     *
     * @return the number of drained elements
     */
    int drainCache() {
        return drain(cacheWanEventQueueMap);
    }

    private int drain(PartitionWanEventQueueMap queueMap) {
        int size = 0;
        for (Map.Entry<String, WanReplicationEventQueue> eventQueueMapEntry : queueMap.entrySet()) {
            WanReplicationEventQueue eventQueue = eventQueueMapEntry.getValue();
            if (eventQueue != null) {
                size += QueueUtil.drainQueue(eventQueue);
            }
        }
        return size;
    }
}
