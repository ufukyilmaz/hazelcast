package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.enterprise.wan.impl.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.ConsistencyCheckResult;

import java.util.Map;

/**
 * Interface for implementations providing WAN sync functionality.
 */
public interface WanPublisherSyncSupport {

    /**
     * Releases all resources for the map with the given {@code mapName}.
     *
     * @param mapName the map name
     */
    void destroyMapData(String mapName);

    /**
     * Processes the WAN sync event.
     *
     * @param event WAN sync event
     * @throws Exception if there was an exception while processing the event
     */
    void processEvent(WanSyncEvent event) throws Exception;

    /**
     * Processes the WAN consistency check event.
     *
     * @param event WAN consistency check  event
     * @throws Exception if there was an exception while processing the event
     */
    void processEvent(WanConsistencyCheckEvent event) throws Exception;

    /**
     * Returns the results of the last WAN consistency checks, grouped by map name.
     */
    Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults();

    /**
     * Returns the statistics about the last synchronization
     */
    Map<String, WanSyncStats> getLastSyncStats();

    /**
     * Decrements the counter for pending WAN sync events.
     *
     * @param sync WAN sync event
     */
    void removeReplicationEvent(EnterpriseMapReplicationObject sync);
}
