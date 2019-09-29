package com.hazelcast.internal.hotrestart;

/**
 * Caches and IMaps config can be created programmatically, but their data must
 * be preloaded on startup. This class keeps data about a named configs that
 * is needed on startup.
 */
public class ConfigDescriptor {

    private final String name;
    private final int id;
    private final String serviceName;

    ConfigDescriptor(String serviceName, String name, int id) {
        this.serviceName = serviceName;
        this.id = id;
        this.name = name;
    }

    public String getServiceName() {
        return serviceName;
    }

    /**
     * Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Cache ID, as encoded in Hot Restart store's key prefix.
     */
    public int getClassId() {
        return id;
    }

    @Override
    public String toString() {
        return "{serviceName '" + serviceName + "' name '" + name + "' ID " + id + '}';
    }
}
