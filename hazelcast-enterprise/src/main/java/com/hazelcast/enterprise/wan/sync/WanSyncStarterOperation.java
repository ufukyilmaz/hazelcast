package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.spi.Operation;

/**
 * Operation to start a WAN synchronization.
 */
public class WanSyncStarterOperation extends Operation {

    private String wanReplicationName;
    private String targetGroupName;
    private WanSyncEvent syncEvent;

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
