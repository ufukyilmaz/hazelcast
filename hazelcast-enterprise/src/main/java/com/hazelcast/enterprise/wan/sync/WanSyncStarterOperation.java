package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractLocalOperation;

/**
 * Operation to start a WAN synchronization.
 * This operation is invoked locally. The local member is then designated
 * as a coordinator member which broadcasts the sync event to all other
 * members.
 */
public class WanSyncStarterOperation extends AbstractLocalOperation implements IdentifiedDataSerializable {

    private String wanReplicationName;
    private String targetGroupName;
    private WanSyncEvent syncEvent;

    public WanSyncStarterOperation() {
    }

    public WanSyncStarterOperation(String wanReplicationName, String targetGroupName, WanSyncEvent syncEvent) {
        this.wanReplicationName = wanReplicationName;
        this.targetGroupName = targetGroupName;
        this.syncEvent = syncEvent;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getService();
        wanReplicationService.populateSyncEventOnMembers(wanReplicationName, targetGroupName, syncEvent);
    }
}
