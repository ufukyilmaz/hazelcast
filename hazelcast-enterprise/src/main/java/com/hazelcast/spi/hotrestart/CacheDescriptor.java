package com.hazelcast.spi.hotrestart;

/**
 * Caches are created programmatically over JCache API, but their data must
 * be preloaded on startup. This class keeps data about a named cache that
 * is needed on startup.
 */
public class CacheDescriptor {
    /** Prepended to the name of a provisional cache instance (before it is
     * programmatically created). */
    private static final String PROVISIONAL_PREFIX = "$HtRst$:";

    private final String name;
    private final int id;
    private final String serviceName;
    private volatile String cachedProvisionalName;

    CacheDescriptor(String serviceName, String name, int id) {
        this.serviceName = serviceName;
        this.id = id;
        this.name = name;
    }

    public String getServiceName() {
        return serviceName;
    }

    /** Cache name. */
    public String getName() {
        return name;
    }

    /** Cache id, as encoded in Hot Restart store's key prefix */
    public int getId() {
        return id;
    }

    public String getProvisionalName() {
        if (cachedProvisionalName == null) {
            cachedProvisionalName = toProvisionalName(name);
        }
        return cachedProvisionalName;
    }

    public static String toProvisionalName(String name) {
        return PROVISIONAL_PREFIX + name;
    }

    public static String toNonProvisionalName(String name) {
        return isProvisionalName(name) ? name.substring(PROVISIONAL_PREFIX.length()) : name;
    }

    public static boolean isProvisionalName(String name) {
        return name.startsWith(PROVISIONAL_PREFIX);
    }

    @Override
    public String toString() {
        return "{serviceName '" + serviceName + "' name '" + name + "' id " + id + '}';
    }
}
