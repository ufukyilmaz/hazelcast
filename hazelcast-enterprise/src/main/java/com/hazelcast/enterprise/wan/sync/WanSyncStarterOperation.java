package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.spi.Operation;

/**
 * Created by emrah on 21/11/2016.
 */
public class WanSyncStarterOperation extends Operation {

    private String wanReplicationName;
    private String targetGroupName;
    private WanSyncEvent syncEvent;

    public WanSyncStarterOperation(String wanReplicationName, String targetGroupName,
                                   WanSyncEvent syncEvent) {
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
