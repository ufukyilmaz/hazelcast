package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapListenerAdaptors;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.util.ConstructorFunction;

/**
 * Responsible for creating {@link com.hazelcast.map.QueryCache QueryCache}
 * specific implementations of listeners.
 */
public final class QueryCacheEventListenerAdapters {

    /**
     * Converts an {@link com.hazelcast.map.listener.EventLostListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> EVENT_LOST_LISTENER_ADAPTER =
            new ConstructorFunction<MapListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(MapListener mapListener) {
                    if (!(mapListener instanceof EventLostListener)) {
                        return null;
                    }
                    final EventLostListener listener = (EventLostListener) mapListener;
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent iMapEvent) {
                            listener.eventLost((EventLostEvent) iMapEvent);
                        }
                    };
                }
            };

    private QueryCacheEventListenerAdapters() {
    }


    static ListenerAdapter[] createQueryCacheListenerAdapters(MapListener mapListener) {
        ListenerAdapter[] mapListenerAdapters = MapListenerAdaptors.createListenerAdapters(mapListener);
        ListenerAdapter eventLostAdapter = EVENT_LOST_LISTENER_ADAPTER.createNew(mapListener);
        ListenerAdapter[] adapters = new ListenerAdapter[mapListenerAdapters.length + 1];
        System.arraycopy(mapListenerAdapters, 0, adapters, 0, mapListenerAdapters.length);
        adapters[mapListenerAdapters.length] = eventLostAdapter;
        return adapters;
    }


    /**
     * Wraps a user defined {@link com.hazelcast.map.listener.MapListener}
     * into a {@link com.hazelcast.map.impl.ListenerAdapter}.
     *
     * @param mapListener a {@link com.hazelcast.map.listener.MapListener} instance.
     * @return {@link com.hazelcast.map.impl.ListenerAdapter} for the user-defined
     * {@link com.hazelcast.map.listener.MapListener}
     */
    public static ListenerAdapter createQueryCacheListenerAdaptor(MapListener mapListener) {
        return new InternalQueryCacheListenerAdapter(mapListener);
    }

}
