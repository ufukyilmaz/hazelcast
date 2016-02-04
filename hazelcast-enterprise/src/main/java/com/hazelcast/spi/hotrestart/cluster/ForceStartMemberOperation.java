package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.hotrestart.HotRestartService;

/**
 * Operation which is sent to members by master to notify that hot restart will continue with force start
 */
public class ForceStartMemberOperation extends AbstractOperation implements JoinOperation {

    @Override
    public void run() throws Exception {
        Address caller = getCallerAddress();
        getLogger().warning("Received force start from: " + caller);

        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveForceStartFromMaster(caller);
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
