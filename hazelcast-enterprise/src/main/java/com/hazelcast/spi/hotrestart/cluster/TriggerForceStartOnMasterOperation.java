package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.hotrestart.HotRestartService;

/**
 * Operation which is sent to master by members to initiate force start process on master
 */
public class TriggerForceStartOnMasterOperation extends AbstractOperation implements JoinOperation {

    @Override
    public void run() throws Exception {
        Address caller = getCallerAddress();
        getLogger().warning("Received force start request from: " + caller);

        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveForceStartTrigger(caller);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return HotRestartService.SERVICE_NAME;
    }

}
