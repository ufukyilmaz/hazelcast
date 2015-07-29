package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.Member;

/**
 * {@link IMapEvent} which holds {@link QueryCacheEventData}
 */
public class SingleIMapEvent implements IMapEvent {

    private final QueryCacheEventData eventData;

    public SingleIMapEvent(QueryCacheEventData eventData) {
        this.eventData = eventData;
    }

    public QueryCacheEventData getEventData() {
        return eventData;
    }

    @Override
    public Member getMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntryEventType getEventType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }
}
