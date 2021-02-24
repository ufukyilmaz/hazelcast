package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * WAN partition queue replacing the old WAN partition queue.
 * <p/>
 * The serialized binary format of this queue is compatible with the legacy
 * WAN queue, therefore, no special action needs to be taken during rolling upgrades.
 * <p/>
 * {@link TwoPhasedLinkedQueue} is extracted as a separate, generic
 * typed class for testability reasons.
 *
 * @since 4.1.2
 */
@SerializableByConvention(PUBLIC_API)
public class WanEventQueue extends TwoPhasedLinkedQueue<FinalizableEnterpriseWanEvent> implements DataSerializable {
    /**
     * The number of backup replicas on which WAN events from this queue are
     * stored
     */
    private int backupCount;

    public WanEventQueue(int backupCount) {
        super();
        this.backupCount = backupCount;
    }

    public WanEventQueue() {
    }

    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        ArrayList<InternalWanEvent> events = collectWanEvents();

        out.writeInt(backupCount);
        out.writeInt(events.size());
        for (InternalWanEvent event : events) {
            out.writeObject(event);
        }
    }

    private ArrayList<InternalWanEvent> collectWanEvents() {
        HashMap<DistributedObjectEntryIdentifier, InternalWanEvent> eventMap = new LinkedHashMap<>();
        List<FinalizableEnterpriseWanEvent> queueElements = new LinkedList<>();
        consumeAll(queueElements::add, event -> {
            DistributedObjectEntryIdentifier id = new DistributedObjectEntryIdentifier(event.getServiceName(),
                    event.getObjectName(), event.getKey());
            eventMap.put(id, event);
        });


        ArrayList<InternalWanEvent> events = new ArrayList<>(eventMap.size() + queueElements.size());
        events.addAll(eventMap.values());
        events.addAll(queueElements);
        return events;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        backupCount = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            FinalizableEnterpriseWanEvent event = in.readObject();
            if (event.getServiceName() != null) {
                offer(event);
            }
        }
    }

}
