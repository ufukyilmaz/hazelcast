package com.hazelcast.enterprise.wan;

import com.hazelcast.wan.WanReplicationEvent;

import java.util.Map;

/**
 * Contains all map/cache event queues of a partition
 */
public class PartitionWanEventContainer {

    private static final String MAP_PREFIX = "M:";
    private static final String CACHE_PREFIX = "J:";

    private final PartitionWanEventQueueMap wanEventQueueMap = new PartitionWanEventQueueMap();

    public PartitionWanEventQueueMap getWanEventQueueMap() {
        return wanEventQueueMap;
    }

    public boolean publishMapWanEvent(String mapName, WanReplicationEvent wanReplicationEvent) {
        String mapNameWithPrefix = MAP_PREFIX + mapName;
        return wanEventQueueMap.offerEvent(wanReplicationEvent, mapNameWithPrefix, getBackupCount(wanReplicationEvent));
    }

    public boolean publishCacheWanEvent(String cacheName, WanReplicationEvent wanReplicationEvent) {
        String cacheNameWithPrefix = CACHE_PREFIX + cacheName;
        return wanEventQueueMap.offerEvent(wanReplicationEvent, cacheNameWithPrefix, getBackupCount(wanReplicationEvent));
    }

    public WanReplicationEvent pollMapWanEvent(String mapName) {
        String mapNameWithPrefix = MAP_PREFIX + mapName;
        return wanEventQueueMap.pollEvent(mapNameWithPrefix);
    }

    public WanReplicationEvent pollCacheWanEvent(String cacheName) {
        String cacheNameWithPrefix = CACHE_PREFIX + cacheName;
        return wanEventQueueMap.pollEvent(cacheNameWithPrefix);
    }

    public WanReplicationEvent pollRandomWanEvent() {
        for (Map.Entry<String, WanReplicationEventQueue> eventQueueMapEntry : wanEventQueueMap.entrySet()) {
            WanReplicationEventQueue eventQueue = eventQueueMapEntry.getValue();
            if (eventQueue != null) {
                WanReplicationEvent wanReplicationEvent = eventQueue.poll();
                if (wanReplicationEvent != null) {
                    return wanReplicationEvent;
                }
            }
        }
        return null;
    }

    private int getBackupCount(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject evObj = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        return evObj.getBackupCount();
    }

    public PartitionWanEventQueueMap getEventQueueMapByBackupCount(int backupCount) {
        PartitionWanEventQueueMap wanEventQueueMap = new PartitionWanEventQueueMap();
        for (Map.Entry<String, WanReplicationEventQueue> entry : wanEventQueueMap.entrySet()) {
            String name = entry.getKey();
            WanReplicationEventQueue queue = entry.getValue();
            if (queue.getBackupCount() >= backupCount) {
                wanEventQueueMap.put(name, queue);
            }
        }
        return wanEventQueueMap;
    }

}
