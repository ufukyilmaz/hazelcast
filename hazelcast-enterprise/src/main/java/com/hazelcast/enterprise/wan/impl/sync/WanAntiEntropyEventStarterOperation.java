package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

/**
 * Operation to start coordination of WAN anti-entropy event publication on
 * a single member.
 * <p>
 * This operation is invoked locally. The local member is then designated
 * as a coordinator member which broadcasts the event to all other members.
 */
public class WanAntiEntropyEventStarterOperation extends AbstractLocalOperation {

    private String wanReplicationName;
    private String targetGroupName;
    private WanAntiEntropyEvent event;

    public WanAntiEntropyEventStarterOperation() {
    }

    public WanAntiEntropyEventStarterOperation(String wanReplicationName,
                                               String targetGroupName,
                                               WanAntiEntropyEvent event) {
        this.wanReplicationName = wanReplicationName;
        this.targetGroupName = targetGroupName;
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getService();
        wanReplicationService.getSyncManager()
                             .publishAntiEntropyEventOnMembers(wanReplicationName, targetGroupName, event);
    }
}
