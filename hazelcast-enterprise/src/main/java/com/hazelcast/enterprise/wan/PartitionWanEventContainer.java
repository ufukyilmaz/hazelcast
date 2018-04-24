package com.hazelcast.enterprise.wan;

import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Map;

/**
 * Contains all map/cache event queues of a partition.
 */
public class PartitionWanEventContainer {

    private static final int DEFAULT_BACKUP_COUNT = 1;

    private final PartitionWanEventQueueMap mapWanEventQueueMap = new PartitionWanEventQueueMap();
    private final PartitionWanEventQueueMap cacheWanEventQueueMap = new PartitionWanEventQueueMap();

    //Below two fields are for random polling
    private PartitionWanEventQueueMap current = mapWanEventQueueMap;
    private PartitionWanEventQueueMap next = cacheWanEventQueueMap;

    /**
     * Publishes the {@code replicationEvent} for the given {@code mapName}
     * map on the partition {@code partitionId}
     *
     * @param mapName             the name of the map for which the event is
     *                            published
     * @param wanReplicationEvent the published replication event
     * @return {@code true} if the element was added to this queue, else
     * {@code false}
     */
    public boolean publishMapWanEvent(String mapName, WanReplicationEvent wanReplicationEvent) {
        return mapWanEventQueueMap.offerEvent(wanReplicationEvent, mapName, getBackupCount(wanReplicationEvent));
    }

    /**
     * Return the head of the wan event queue for the {@code mapName}
     *
     * @param mapName the map for which an event is polled
     * @return the replication event
     */
    public WanReplicationEvent pollMapWanEvent(String mapName) {
        return mapWanEventQueueMap.pollEvent(mapName);
    }

    /**
     * Return the head of the wan event queue for the {@code cacheName}
     *
     * @param cacheName the cache for which an event is polled
     * @return the replication event
     */
    public WanReplicationEvent pollCacheWanEvent(String cacheName) {
        return cacheWanEventQueueMap.pollEvent(cacheName);
    }

    /**
     * Returns an event from a random map or cache queue or {@code null} if
     * there are no events.
     *
     * @return a WAN event or {@code null} if there are none
     */
    public WanReplicationEvent pollRandomWanEvent() {
        WanReplicationEvent event = pollRandomWanEvent(current);
        if (event != null) {
            PartitionWanEventQueueMap temp = current;
            current = next;
            next = temp;
        } else {
            event = pollRandomWanEvent(next);
        }
        return event;
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
     * Returns an event from a random queue in the {@code eventQueueMap} or
     * {@code null} if there are no events.
     *
     * @return a WAN event or {@code null} if there are none
     */
    private WanReplicationEvent pollRandomWanEvent(PartitionWanEventQueueMap eventQueueMap) {
        for (WanReplicationEventQueue eventQueue : eventQueueMap.values()) {
            if (eventQueue != null) {
                WanReplicationEvent wanReplicationEvent = eventQueue.poll();
                if (wanReplicationEvent != null) {
                    return wanReplicationEvent;
                }
            }
        }
        return null;
    }

    /**
     * Returns the backup count for the {@code wanReplicationEvent}
     * representing the number of backup replicas on which this event will be
     * stored in addition to being stored on the primary replica.
     *
     * @param wanReplicationEvent the WAN event
     * @return the number of backup replicas on which this event is stored
     */
    private int getBackupCount(WanReplicationEvent wanReplicationEvent) {
        ReplicationEventObject eventObject = wanReplicationEvent.getEventObject();
        if (eventObject instanceof EnterpriseReplicationEventObject) {
            EnterpriseReplicationEventObject evObj = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
            return evObj.getBackupCount();
        } else {
            return DEFAULT_BACKUP_COUNT;
        }
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
     * @param cacheName           the name of the cache for which the event is
     *                            published
     * @param wanReplicationEvent the published replication event
     * @return {@code true} if the element was added to this queue, else
     * {@code false}
     */
    public boolean publishCacheWanEvent(String cacheName, WanReplicationEvent wanReplicationEvent) {
        return cacheWanEventQueueMap.offerEvent(wanReplicationEvent, cacheName, getBackupCount(wanReplicationEvent));
    }

    public void clear() {
        mapWanEventQueueMap.clear();
        cacheWanEventQueueMap.clear();
    }
}
