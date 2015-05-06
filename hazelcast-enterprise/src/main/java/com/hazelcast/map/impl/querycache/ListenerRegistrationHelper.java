package com.hazelcast.map.impl.querycache;

/**
 * Contains helper method for creating listener registration name. This helper will be used to provide
 * a unique name for subscriber listeners. We want to register that listener to event service with a unique name.
 */
public final class ListenerRegistrationHelper {

    private static final String PLACE_HOLDER = "::::";

    private ListenerRegistrationHelper() {
    }

    public static String generateListenerName(String mapName, String cacheName) {
        return mapName + PLACE_HOLDER + cacheName;
    }
}
