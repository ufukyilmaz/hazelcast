package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Identifier for a specific entry in a distributed object (e.g. specific
 * map or cache). This can be used as a composite key to identify a single
 * entry in the cluster.
 */
class DistributedObjectEntryIdentifier {
    private final String serviceName;
    private final String objectName;
    private final Data key;

    DistributedObjectEntryIdentifier(String serviceName, String objectName, Data key) {
        this.serviceName = checkNotNull(serviceName, "Service name must not be null");
        this.objectName = checkNotNull(objectName, "Object name must not be null");
        this.key = checkNotNull(key, "Entry key must not be null");
    }

    @Override
    @SuppressWarnings("checkstyle:innerassignment")
    public boolean equals(Object o) {
        final DistributedObjectEntryIdentifier that;
        return this == o || o instanceof DistributedObjectEntryIdentifier
                && this.serviceName.equals((that = (DistributedObjectEntryIdentifier) o).serviceName)
                && this.objectName.equals(that.objectName)
                && this.key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = serviceName.hashCode();
        result = 31 * result + objectName.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
