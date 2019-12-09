package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * WAN event queue map of a partition and a specific service (map/cache).
 * Contains all map/cache event queues of a partition.
 */
@SerializableByConvention(PUBLIC_API)
public class PartitionWanEventQueueMap extends ConcurrentHashMap<String, WanEventQueue> implements DataSerializable {
    private static final long serialVersionUID = 1L;

    /** The mutex for concurrently creating new instances of WAN queues */
    private final transient Object mutex = new Object();

    /**
     * Publishes the {@code replicationEvent}
     *
     * @param distributedObjectName the name of the distributed object for
     *                              which this event is published
     * @param backupCount           the number of backup replicas on which this WAN
     *                              event is stored
     * @param wanReplicationEvent   the published replication event
     * @return {@code true} if the element was added to this queue, else
     * {@code false}
     */
    public boolean offerEvent(InternalWanEvent wanReplicationEvent,
                              String distributedObjectName,
                              int backupCount) {
        return getOrCreateEventQueue(distributedObjectName, backupCount).offer(wanReplicationEvent);
    }

    /**
     * Returns a WAN event for the given distributed object or {@code null} if
     * there is none.
     *
     * @param distributedObjectName the name of the distributed object
     * @return the WAN event
     */
    public InternalWanEvent pollEvent(String distributedObjectName) {
        WanEventQueue eventQueue = get(distributedObjectName);
        if (eventQueue != null) {
            return eventQueue.poll();
        }
        return null;
    }

    /**
     * Gets a WAN event queue for the distributed object or creates one if it
     * does not exist.
     * This method may be called concurrently.
     *
     * @param distributedObjectName the name of the distributed object
     * @param backupCount           the number of backup replicas on which
     *                              events from this queue are stored
     * @return the WAN event queue
     */
    private WanEventQueue getOrCreateEventQueue(String distributedObjectName, int backupCount) {
        WanEventQueue eventQueue = get(distributedObjectName);
        if (eventQueue == null) {
            synchronized (mutex) {
                eventQueue = get(distributedObjectName);
                if (eventQueue == null) {
                    eventQueue = new WanEventQueue(backupCount);
                    put(distributedObjectName, eventQueue);
                }
            }
        }
        return eventQueue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(size());
        for (Map.Entry<String, WanEventQueue> entry : entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            WanEventQueue queue = in.readObject();
            put(name, queue);
        }
    }
}
