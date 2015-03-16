package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.ReplicationEventObject;

/**
 * Marker interface enterprise wan replication events
 */
public interface EnterpriseReplicationEventObject extends ReplicationEventObject {

    String getGroupName();
    Data getKey();

}
