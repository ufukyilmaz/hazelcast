package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationMerkleTreeNode;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationSync;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

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

/**
 * Container to gather batch of {@link InternalWanReplicationEvent} objects.
 * If {@link WanBatchReplicationPublisherConfig#isSnapshotEnabled()}
 * is {@code true}, only the latest event for a key will be sent.
 *
 * @see com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication
 */
public class BatchWanReplicationEvent implements IdentifiedDataSerializable {
    private transient boolean snapshotEnabled;
    private transient Map<DistributedObjectEntryIdentifier, InternalWanReplicationEvent> eventMap;
    private transient List<InternalWanReplicationEvent> coalescedEvents;

    private transient int primaryEventCount;
    private transient int totalEntryCount;

    private Collection<InternalWanReplicationEvent> eventList;


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
    public void addEvent(InternalWanReplicationEvent event) {
        boolean isCoalesced = false;
        if (snapshotEnabled) {
            DistributedObjectEntryIdentifier id = getDistributedObjectEntryIdentifier(event);
            InternalWanReplicationEvent coalescedEvent = eventMap.put(id, event);
            if (coalescedEvent != null) {
                coalescedEvents.add(coalescedEvent);
                isCoalesced = true;
            }
        } else {
            eventList.add(event);
        }

        if (!isCoalesced) {
            incrementEventCount(event);
        }

        if (!(event instanceof EnterpriseMapReplicationMerkleTreeNode)
                && !(event instanceof EnterpriseMapReplicationSync)) {
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
    private void incrementEventCount(InternalWanReplicationEvent eventObject) {
        if (eventObject instanceof EnterpriseMapReplicationMerkleTreeNode) {
            EnterpriseMapReplicationMerkleTreeNode node = (EnterpriseMapReplicationMerkleTreeNode) eventObject;
            totalEntryCount += node.getEntryCount();
        } else {
            totalEntryCount++;
        }
    }

    private DistributedObjectEntryIdentifier getDistributedObjectEntryIdentifier(WanReplicationEvent event) {
        InternalWanReplicationEvent eventObject = (InternalWanReplicationEvent) event;
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
    public List<InternalWanReplicationEvent> getCoalescedEvents() {
        return coalescedEvents;
    }

    /** Returns the WAN events in this batch */
    public Collection<InternalWanReplicationEvent> getEvents() {
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
}
