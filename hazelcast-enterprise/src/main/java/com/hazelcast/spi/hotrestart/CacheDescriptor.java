package com.hazelcast.spi.hotrestart;

/**
 * Caches are created programmatically over JCache API, but their data must
 * be preloaded on startup. This class keeps data about a named cache that
 * is needed on startup.
 */
public class CacheDescriptor {

    private final String name;
    private final int id;
    private final String serviceName;

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

    @Override
    public String toString() {
        return "{serviceName '" + serviceName + "' name '" + name + "' id " + id + '}';
    }
}
