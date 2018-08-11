package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEventResult;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.wan.merkletree.ConsistencyCheckResult;

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
     * @param event  WAN sync event
     * @param result the result of the processing
     * @throws Exception if there was an exception while processing the event
     */
    void processEvent(WanSyncEvent event, WanAntiEntropyEventResult result) throws Exception;

    /**
     * Processes the WAN consistency check event.
     *
     * @param event  WAN consistency check  event
     * @param result the result of the processing
     * @throws Exception if there was an exception while processing the event
     */
    void processEvent(WanConsistencyCheckEvent event, WanAntiEntropyEventResult result) throws Exception;

    /**
     * Returns the results of the last WAN consistency checks, grouped by map name.
     */
    Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults();

    /**
     * Decrements the counter for pending WAN sync events.
     *
     * @param sync WAN sync event
     */
    void removeReplicationEvent(EnterpriseMapReplicationObject sync);
}
