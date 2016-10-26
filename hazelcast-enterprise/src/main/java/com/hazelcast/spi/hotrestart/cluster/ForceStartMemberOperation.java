package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartService;

/**
 * Operation which is sent to members by master to notify that Hot Restart will continue with force start.
 */
public class ForceStartMemberOperation extends Operation implements JoinOperation {

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

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartClusterSerializerHook.FORCE_START_MEMBER;
    }
}
