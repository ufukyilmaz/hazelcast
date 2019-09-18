package com.hazelcast.enterprise.wan.impl;

/**
 * WAN sync types. These define the scope of entries which need to be synced.
 */
public enum WanSyncType {

    /**
     * Sync all entries for all maps.
     */
    ALL_MAPS(0),

    /**
     * Sync all entries for a specific map.
     */
    SINGLE_MAP(1);

    private int type;

    WanSyncType(final int type) {
        this.type = type;
    }

    /**
     * @return unique ID of the event type
     */
    public int getType() {
        return type;
    }

    public static WanSyncType getByType(final int eventType) {
        for (WanSyncType entryEventType : values()) {
            if (entryEventType.type == eventType) {
                return entryEventType;
            }
        }
        return null;
    }
}
