package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.listener.MapListener;

import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdapters;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Internal listener adapter for the listener of a {@code QueryCache}
 */
public class InternalQueryCacheListenerAdapter implements ListenerAdapter {

    private final ListenerAdapter[] listenerAdapters;

    InternalQueryCacheListenerAdapter(MapListener mapListener) {
        checkNotNull(mapListener, "mapListener cannot be null");

        this.listenerAdapters = createQueryCacheListenerAdapters(mapListener);
    }

    @Override
    public void onEvent(IMapEvent event) {
        EntryEventType eventType = event.getEventType();

        if (eventType != null) {
            callListener(event, eventType.getType());
            return;
        }

        if (event instanceof EventLostEvent) {
            EventLostEvent eventLostEvent = (EventLostEvent) event;
            callListener(eventLostEvent, EventLostEvent.EVENT_TYPE);
            return;
        }

    }

    private void callListener(IMapEvent event, int eventType) {
        int adapterIndex = Integer.numberOfTrailingZeros(eventType);
        ListenerAdapter listenerAdapter = listenerAdapters[adapterIndex];
        if (listenerAdapter == null) {
            return;
        }
        listenerAdapter.onEvent(event);
    }
}
