package com.hazelcast.enterprise.wan;

import com.hazelcast.instance.Node;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains all map/cache event queues
 */
public class WanReplicationEventQueueContainer {

    private static final String MAP_PREFIX = "M:";
    private static final String CACHE_PREFIX = "J:";

    ConcurrentHashMap<String, EventQueueMap> container = new ConcurrentHashMap<String, EventQueueMap>();
    private Object mutex = new Object();
    private Node node;

    public WanReplicationEventQueueContainer(Node node) {
        this.node = node;
    }

    public boolean publishMapWanEvent(String mapName, int partitionId, WanReplicationEvent wanReplicationEvent) {
        String mapNameWithPrefix = MAP_PREFIX + mapName;
        Map<Integer, WanReplicationEventQueue> eventQueueMap
                = getEventQueueMap(mapNameWithPrefix, getBackupCount(wanReplicationEvent));
        return eventQueueMap.get(partitionId).offer(wanReplicationEvent);
    }

    public boolean publishCacheWanEvent(String cacheName, int partitionId, WanReplicationEvent wanReplicationEvent) {
        String cacheNameWithPrefix = CACHE_PREFIX + cacheName;
        Map<Integer, WanReplicationEventQueue> eventQueueMap
                = getEventQueueMap(cacheNameWithPrefix, getBackupCount(wanReplicationEvent));
        return eventQueueMap.get(partitionId).offer(wanReplicationEvent);
    }

    public WanReplicationEvent pollMapWanEvent(String mapName, int partitionId) {
        String mapNameWithPrefix = MAP_PREFIX + mapName;
        Map<Integer, WanReplicationEventQueue> eventQueueMap = getEventQueueMap(mapNameWithPrefix);
        if (eventQueueMap != null) {
            return eventQueueMap.get(partitionId).poll();
        }
        return null;
    }

    public WanReplicationEvent pollCacheWanEvent(String cacheName, int partitionId) {
        String cacheNameWithPrefix = CACHE_PREFIX + cacheName;
        Map<Integer, WanReplicationEventQueue> eventQueueMap = getEventQueueMap(cacheNameWithPrefix);
        if (eventQueueMap != null) {
            return eventQueueMap.get(partitionId).poll();
        }
        return null;
    }

    public WanReplicationEvent pollRandomWanEvent(int partitionId) {
        for (String name : container.keySet()) {
            WanReplicationEventQueue eventQueue = container.get(name).get(partitionId);
            WanReplicationEvent wanReplicationEvent = eventQueue.poll();
            if (wanReplicationEvent != null) {
                return wanReplicationEvent;
            }
        }
        return null;
    }

    public Map<String, WanReplicationEventQueue> getEventQueueMapByPartitionId(int partitionId) {
        Map<String, WanReplicationEventQueue> queueMap = new HashMap<String, WanReplicationEventQueue>();
        for (Map.Entry<String, EventQueueMap> mapEntry : container.entrySet()) {
            WanReplicationEventQueue eventQueue = mapEntry.getValue().get(partitionId);
            if (eventQueue != null) {
                queueMap.put(mapEntry.getKey(), eventQueue);
            }
        }
        return queueMap;
    }

    private int getBackupCount(WanReplicationEvent wanReplicationEvent) {
        EnterpriseReplicationEventObject evObj = (EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject();
        return evObj.getBackupCount();
    }

    private Map<Integer, WanReplicationEventQueue> getEventQueueMap(String name, int backupCount) {
        EventQueueMap eventQueueMap = container.get(name);
        if (eventQueueMap == null) {
            synchronized (mutex) {
                eventQueueMap = container.get(name);
                if (eventQueueMap == null) {
                    eventQueueMap = new EventQueueMap(backupCount);
                    for (InternalPartition partition : node.getPartitionService().getPartitions()) {
                        WanReplicationEventQueue eventQueue = new WanReplicationEventQueue();
                        eventQueueMap.put(partition.getPartitionId(), eventQueue);
                    }
                    container.put(name, eventQueueMap);
                }
            }
        }
        return eventQueueMap;
    }

    public Map<Integer, WanReplicationEventQueue> getEventQueueMap(String name) {
        return container.get(name);
    }

    public ConcurrentHashMap<String, EventQueueMap> getContainer() {
        return container;
    }
}
