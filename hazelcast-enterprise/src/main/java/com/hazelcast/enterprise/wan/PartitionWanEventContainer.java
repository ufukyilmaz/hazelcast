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

    public boolean publishMapWanEvent(String mapName, WanReplicationEvent wanReplicationEvent) {
        return mapWanEventQueueMap.offerEvent(wanReplicationEvent, mapName, getBackupCount(wanReplicationEvent));
    }

    public WanReplicationEvent pollMapWanEvent(String mapName) {
        return mapWanEventQueueMap.pollEvent(mapName);
    }

    public WanReplicationEvent pollCacheWanEvent(String cacheName) {
        return cacheWanEventQueueMap.pollEvent(cacheName);
    }

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

    private WanReplicationEvent pollRandomWanEvent(PartitionWanEventQueueMap eventQueueMap) {
        for (Map.Entry<String, WanReplicationEventQueue> eventQueueMapEntry : eventQueueMap.entrySet()) {
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

    public boolean publishCacheWanEvent(String cacheName, WanReplicationEvent wanReplicationEvent) {
        return cacheWanEventQueueMap.offerEvent(wanReplicationEvent, cacheName, getBackupCount(wanReplicationEvent));
    }

    public void clear() {
        mapWanEventQueueMap.clear();
        cacheWanEventQueueMap.clear();
    }
}
