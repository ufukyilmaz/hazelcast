package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.wan.ReplicationEventObject;

import java.util.Set;

/**
 * Marker interface for enterprise wan replication events
 */
public interface EnterpriseReplicationEventObject extends ReplicationEventObject, DataSerializable {

    Set<String> getGroupNames();
    Data getKey();
    int getBackupCount();
}
