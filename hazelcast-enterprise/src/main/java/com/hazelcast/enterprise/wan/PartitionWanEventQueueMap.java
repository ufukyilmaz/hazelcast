package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wan event queue map of a partition
 * Contains all map/cache event queues of a partition
 */
public class PartitionWanEventQueueMap extends ConcurrentHashMap<String, WanReplicationEventQueue> implements DataSerializable {

    private static final long serialVersionUID = 1L;

    private transient Object mutex = new Object();

    public boolean offerEvent(WanReplicationEvent wanReplicationEvent, String dataStructure, int backupCount) {
        WanReplicationEventQueue wanReplicationEventQueue = getOrCreateEventQueue(dataStructure, backupCount);
        return wanReplicationEventQueue.offer(wanReplicationEvent);
    }

    public WanReplicationEvent pollEvent(String dataStructureName) {
        WanReplicationEventQueue eventQueue = get(dataStructureName);
        if (eventQueue != null) {
            return eventQueue.poll();
        }
        return null;
    }

    private WanReplicationEventQueue getOrCreateEventQueue(String dataStructureName, int backupCount) {
        WanReplicationEventQueue eventQueue =  get(dataStructureName);
        if (eventQueue == null) {
            synchronized (mutex) {
                eventQueue = get(dataStructureName);
                if (eventQueue == null) {
                    eventQueue = new WanReplicationEventQueue(backupCount);
                    put(dataStructureName, eventQueue);
                }
            }
        }
        return  eventQueue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(size());
        for (Map.Entry<String, WanReplicationEventQueue> entry : entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            WanReplicationEventQueue queue = in.readObject();
            put(name, queue);
        }
    }
}
