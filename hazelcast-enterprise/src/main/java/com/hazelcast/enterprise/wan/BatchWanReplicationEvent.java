package com.hazelcast.enterprise.wan;

import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Container to gather batch of {@link WanReplicationEvent} objects.
 * If {@link com.hazelcast.enterprise.wan.replication.WanReplicationProperties#SNAPSHOT_ENABLED} is true, only the
 * latest event for a key will be sent.
 */
public class BatchWanReplicationEvent implements IdentifiedDataSerializable {
    private boolean snapshotEnabled;
    private transient Map<DistributedObjectEntryIdentifier, WanReplicationEvent> eventMap;
    private List<WanReplicationEvent> eventList;
    private int addedEventCount;

    public BatchWanReplicationEvent() {
    }

    public BatchWanReplicationEvent(boolean snapshotEnabled) {
        this.snapshotEnabled = snapshotEnabled;
        if (snapshotEnabled) {
            eventMap = new HashMap<DistributedObjectEntryIdentifier, WanReplicationEvent>();
        } else {
            eventList = new ArrayList<WanReplicationEvent>();
        }
    }

    public void addEvent(WanReplicationEvent event) {
        final EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) event.getEventObject();
        if (snapshotEnabled) {
            final DistributedObjectEntryIdentifier id = new DistributedObjectEntryIdentifier(
                    event.getServiceName(), eventObject.getObjectName(), eventObject.getKey());
            eventMap.put(id, event);
        } else {
            eventList.add(event);
        }
        if (!(eventObject instanceof EnterpriseMapReplicationSync)) {
            // sync events don't count against primary WAN counters
            addedEventCount++;
        }
    }

    /**
     * Returns the number of events added to this batch. This may be different
     * than the size of the batch if {@link #snapshotEnabled} is {@code true}.
     */
    public int getAddedEventCount() {
        return addedEventCount;
    }

    /** Returns the WAN events in this batch */
    public Collection<WanReplicationEvent> getEvents() {
        if (snapshotEnabled) {
            return eventMap == null
                    ? Collections.<WanReplicationEvent>emptyList()
                    : eventMap.values();
        }
        return eventList == null
                ? Collections.<WanReplicationEvent>emptyList()
                : eventList;
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
        // we write out false for backwards compatibility when replicating to a
        // 3.8+ cluster. We choose false as the target cluster does not need
        // to be concerned with event coalescing on the source cluster
        out.writeBoolean(false);
        // this should be optimised in the future release to only
        // write out the size+items, instead of type+size+items.
        // right now we keep this for compatibility with 3.8+ clusters
        // Also, we can then make the getEvents() return Collection then
        // Now the writeObject cannot serialise the Map.values() type
        final Collection<WanReplicationEvent> events = getEvents();
        final ArrayList<WanReplicationEvent> eventList = events instanceof ArrayList
                ? (ArrayList<WanReplicationEvent>) events
                : new ArrayList<WanReplicationEvent>(events);
        out.writeObject(eventList);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        in.readBoolean();
        eventList = in.readObject();
    }

    /**
     * Identifier for a specific entry in a distributed object (e.g. specific
     * map or cache). This can be used as a composite key to identify a single
     * entry in the cluster.
     */
    private static class DistributedObjectEntryIdentifier {
        private final String serviceName;
        private final String objectName;
        private final Data key;

        DistributedObjectEntryIdentifier(String serviceName, String objectName, Data key) {
            this.serviceName = checkNotNull(serviceName, "Service name must not be null");
            this.objectName = checkNotNull(objectName, "Object name must not be null");
            this.key = checkNotNull(key, "Entry key must not be null");
        }

        @Override
        @SuppressWarnings("checkstyle:innerassignment")
        public boolean equals(Object o) {
            final DistributedObjectEntryIdentifier that;
            return this == o || o instanceof DistributedObjectEntryIdentifier
                    && this.serviceName.equals((that = (DistributedObjectEntryIdentifier) o).serviceName)
                    && this.objectName.equals(that.objectName)
                    && this.key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = serviceName.hashCode();
            result = 31 * result + objectName.hashCode();
            result = 31 * result + key.hashCode();
            return result;
        }
    }
}
