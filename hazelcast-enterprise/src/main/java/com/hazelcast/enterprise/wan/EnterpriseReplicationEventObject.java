package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.wan.ReplicationEventObject;

import java.util.Set;

/**
 * Marker interface for enterprise WAN replication events.
 */
public interface EnterpriseReplicationEventObject extends ReplicationEventObject, DataSerializable {
    /**
     * Returns the name of the distributed object (map or cache) on which this
     * event occurred.
     *
     * @return the distributed object name
     */
    String getObjectName();

    Set<String> getGroupNames();

    int getBackupCount();
    long getCreationTime();
}
