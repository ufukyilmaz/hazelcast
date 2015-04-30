package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container to gather batch of {@link WanReplicationEvent} objects
 */
public class BatchWanReplicationEvent implements IdentifiedDataSerializable {

    private boolean snapshotEnabled;
    private transient Map<Data, WanReplicationEvent> eventMap
            = new ConcurrentHashMap<Data, WanReplicationEvent>();
    private List<WanReplicationEvent> eventList = new ArrayList<WanReplicationEvent>();

    public BatchWanReplicationEvent() {
    }

    public BatchWanReplicationEvent(boolean snapshotEnabled) {
        this.snapshotEnabled = snapshotEnabled;
    }

    public void addEvent(WanReplicationEvent event) {
        if (snapshotEnabled) {
            EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) event.getEventObject();
            eventMap.put(eventObject.getKey(), event);
        } else {
            eventList.add(event);
        }
    }

    public List<WanReplicationEvent> getEventList() {
        if (snapshotEnabled) {
            eventList.addAll(eventMap.values());
        }
        return eventList;
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.BATCH_WAN_REP_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(snapshotEnabled);
        out.writeObject(eventList);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        snapshotEnabled = in.readBoolean();
        eventList = in.readObject();
    }
}
