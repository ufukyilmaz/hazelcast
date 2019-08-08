package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Set;

/**
 * Marker interface for enterprise WAN replication events.
 */
public interface EnterpriseReplicationEventObject extends WanReplicationEvent, DataSerializable {
    /**
     * Returns the name of the distributed object (map or cache) on which this
     * event occurred.
     *
     * @return the distributed object name
     */
    String getObjectName();

    /**
     * Returns the set of cluster group names on which this event has already
     * been processed.
     */
    Set<String> getGroupNames();

    /**
     * Returns the backup count (sync and async) for this WAN event.
     */
    int getBackupCount();

    /**
     * Returns the creation time for this event in milliseconds.
     *
     * @see Clock#currentTimeMillis()
     */
    long getCreationTime();
}
