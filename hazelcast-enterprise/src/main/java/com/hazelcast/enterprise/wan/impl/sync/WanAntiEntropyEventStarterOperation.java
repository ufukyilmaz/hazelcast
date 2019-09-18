package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.impl.AbstractWanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

/**
 * Operation to start coordination of WAN anti-entropy event publication on
 * a single member.
 * <p>
 * This operation is invoked locally. The local member is then designated
 * as a coordinator member which broadcasts the event to all other members.
 */
public class WanAntiEntropyEventStarterOperation extends AbstractLocalOperation implements AllowedDuringPassiveState {

    private String wanReplicationName;
    private String wanPublisherId;
    private AbstractWanAntiEntropyEvent event;

    public WanAntiEntropyEventStarterOperation() {
    }

    public WanAntiEntropyEventStarterOperation(String wanReplicationName,
                                               String wanPublisherId,
                                               AbstractWanAntiEntropyEvent event) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.event = event;
    }

    @Override
    public void run() throws Exception {
        EnterpriseWanReplicationService wanReplicationService = getService();
        wanReplicationService.getSyncManager()
                             .publishAntiEntropyEventOnMembers(wanReplicationName, wanPublisherId, event);
    }
}
