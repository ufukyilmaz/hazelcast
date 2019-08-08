package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Container to gather batch of {@link WanReplicationEvent} objects.
 * If {@link WanBatchReplicationPublisherConfig#isSnapshotEnabled()}
 * is {@code true}, only the latest event for a key will be sent.
 *
 * @see com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication
 */
public class BatchWanReplicationEvent implements IdentifiedDataSerializable {
    private transient boolean snapshotEnabled;
    private transient Map<DistributedObjectEntryIdentifier, WanReplicationEvent> eventMap;
    private transient List<WanReplicationEvent> coalescedEvents;

    private transient int primaryEventCount;
    private transient int totalEntryCount;

    private Collection<WanReplicationEvent> eventList;


    public BatchWanReplicationEvent() {
    }

    public BatchWanReplicationEvent(boolean snapshotEnabled) {
        this.snapshotEnabled = snapshotEnabled;
        if (snapshotEnabled) {
            eventMap = new HashMap<>();
            coalescedEvents = new LinkedList<>();
        } else {
            eventList = new ArrayList<>();
            coalescedEvents = Collections.emptyList();
        }
    }


    /**
     * Adds a WAN event to the batch. Depending on the whether snapshot is
     * enabled, the event may be coalesced with an another event with the same
     * key or not.
     *
     * @param event a WAN replication event
     * @see WanBatchReplicationPublisherConfig#isSnapshotEnabled()
     */
    public void addEvent(WanReplicationEvent event) {
        boolean isCoalesced = false;
        if (snapshotEnabled) {
            DistributedObjectEntryIdentifier id = getDistributedObjectEntryIdentifier(event);
            WanReplicationEvent coalescedEvent = eventMap.put(id, event);
            if (coalescedEvent != null) {
                coalescedEvents.add(coalescedEvent);
                isCoalesced = true;
            }
        } else {
            eventList.add(event);
        }

        EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) event;
        if (!isCoalesced) {
            incrementEventCount(eventObject);
        }

        if (!(eventObject instanceof EnterpriseMapReplicationMerkleTreeNode)
                && !(eventObject instanceof EnterpriseMapReplicationSync)) {
            // sync events don't count against primary WAN counters
            primaryEventCount++;
        }
    }

    /**
     * Increments the total number of WAN events contained in this batch.
     * A merkle tree node is counted as the number of entries it contains.
     *
     * @param eventObject the WAN event object
     */
    private void incrementEventCount(EnterpriseReplicationEventObject eventObject) {
        if (eventObject instanceof EnterpriseMapReplicationMerkleTreeNode) {
            EnterpriseMapReplicationMerkleTreeNode node = (EnterpriseMapReplicationMerkleTreeNode) eventObject;
            totalEntryCount += node.getEntryCount();
        } else {
            totalEntryCount++;
        }
    }

    private DistributedObjectEntryIdentifier getDistributedObjectEntryIdentifier(WanReplicationEvent event) {
        EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) event;
        return new DistributedObjectEntryIdentifier(event.getServiceName(), eventObject.getObjectName(), eventObject.getKey());
    }

    /**
     * Returns the number of non-sync WAN events added to this batch.
     * This may be different than the size of the {@link #getEvents()} if
     * {@link #snapshotEnabled} is {@code true}.
     */
    public int getPrimaryEventCount() {
        return primaryEventCount;
    }

    /**
     * Returns the total number of WAN events in this batch, including WAN sync
     * events and merkle tree node entries.
     * This may differ from other counts, such as the size of the
     * {@link #getEvents()} and the {@link #getPrimaryEventCount()}.
     */
    public int getTotalEntryCount() {
        return totalEntryCount;
    }

    /**
     * Returns all events which were coalesced by other newer events in
     * {@link #getEvents()}.
     */
    public List<WanReplicationEvent> getCoalescedEvents() {
        return coalescedEvents;
    }

    /** Returns the WAN events in this batch */
    public Collection<WanReplicationEvent> getEvents() {
        if (snapshotEnabled) {
            return eventMap == null
                    ? Collections.emptyList()
                    : eventMap.values();
        }
        return eventList == null
                ? Collections.emptyList()
                : eventList;
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.BATCH_WAN_REP_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeCollection(getEvents(), out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventList = readCollection(in);
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
