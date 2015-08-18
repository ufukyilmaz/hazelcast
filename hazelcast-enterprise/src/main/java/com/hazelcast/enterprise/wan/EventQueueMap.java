package com.hazelcast.enterprise.wan;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Partition based event queue map
 */
public class EventQueueMap extends ConcurrentHashMap<Integer, WanReplicationEventQueue> {

    private int backupCount;

    public EventQueueMap(int backupCount) {
        this.backupCount = backupCount;
    }

}
