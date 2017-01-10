package com.hazelcast.enterprise.wan.sync;

/**
 * WAN sync types
 */
public enum WanSyncType {

    /**
     * Sync all maps
     */
    ALL_MAPS(0),

    /**
     * Sync only a specific map
     */
    SINGLE_MAP(1);

    private int type;

    WanSyncType(final int type) {
        this.type = type;
    }

    /**
     * @return unique id of the event type.
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
