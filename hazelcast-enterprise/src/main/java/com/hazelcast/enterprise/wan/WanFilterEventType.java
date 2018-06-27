package com.hazelcast.enterprise.wan;

import com.hazelcast.map.wan.filter.MapWanEventFilter;

/**
 * Event types to be used by {@link com.hazelcast.cache.wan.filter.CacheWanEventFilter}
 * and {@link MapWanEventFilter} instances.
 */
public enum WanFilterEventType {

    /**
     * An event type indicating that the related entry was created or updated.
     */
    UPDATED(1),

    /**
     * An event type indicating that the entry was removed.
     */
    REMOVED(2),

    /**
     * An event type indicating that the entry was loaded.
     */
    LOADED(3);

    private int type;

    WanFilterEventType(final int type) {
        this.type = type;
    }

    /**
     * @return unique ID of the event type
     */
    public int getType() {
        return type;
    }
}
