package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * Serializable WAN replication event queue wrapper containing the number
 * of backup replicas for the events in this queue.
 */
@SerializableByConvention(PUBLIC_API)
public class WanEventQueue extends LinkedBlockingQueue<InternalWanEvent>
        implements DataSerializable {

    /**
     * The number of backup replicas on which WAN events from this queue are
     * stored
     */
    private int backupCount;

    public WanEventQueue() {

    }

    public WanEventQueue(int backupCount) {
        this.backupCount = backupCount;
    }

    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(backupCount);
        InternalWanEvent[] events = toArray(new InternalWanEvent[0]);
        out.writeInt(events.length);
        for (InternalWanEvent event : events) {
            out.writeObject(event);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        backupCount = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            InternalWanEvent event = in.readObject();
            if (event.getServiceName() != null) {
                offer(event);
            }
        }
    }
}
