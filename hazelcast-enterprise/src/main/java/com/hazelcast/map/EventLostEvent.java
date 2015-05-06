package com.hazelcast.map;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.Member;

/**
 * This event is fired in case of an event lost detection.
 * The fired event can be caught by implementing {@link com.hazelcast.map.listener.EventLostListener EventLostListener}
 *
 * @see com.hazelcast.map.listener.EventLostListener
 */
public class EventLostEvent implements IMapEvent {

    // TODO EntryEvenType extensibility.
    /**
     * Event type id.
     *
     * @see EntryEventType
     */
    public static final int EVENT_TYPE = getNextEntryEventTypeId();

    private final int partitionId;

    private final String source;

    private final Member member;

    public EventLostEvent(String source, Member member, int partitionId) {
        this.source = source;
        this.member = member;
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Returns next event type id.
     *
     * @return next event type id.
     * @see EntryEventType
     */
    private static int getNextEntryEventTypeId() {
        int higherTypeId = Integer.MIN_VALUE;
        int i = 0;
        EntryEventType[] values = EntryEventType.values();
        for (EntryEventType value : values) {
            int typeId = value.getType();
            if (i == 0) {
                higherTypeId = typeId;
            } else {
                if (typeId > higherTypeId) {
                    higherTypeId = typeId;
                }
            }
            i++;
        }
        return ++higherTypeId;
    }

    @Override
    public Member getMember() {
        return member;
    }

    /**
     * Intentionally returns null.
     * Used in {@link com.hazelcast.map.impl.querycache.subscriber.InternalQueryCacheListenerAdapter}
     *
     * @return null.
     * @see com.hazelcast.map.impl.querycache.subscriber.InternalQueryCacheListenerAdapter
     */
    @Override
    public EntryEventType getEventType() {
        return null;
    }

    @Override
    public String getName() {
        return source;
    }
}

