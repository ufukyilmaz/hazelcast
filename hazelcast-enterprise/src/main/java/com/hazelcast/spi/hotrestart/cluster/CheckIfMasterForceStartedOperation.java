package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartService;

import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;

/**
 * Operation which is sent to master by members while waiting all members to join.
 * This operation is necessary since waiting members will not notice if force start is triggered while waiting all members to join
 */
public class CheckIfMasterForceStartedOperation extends Operation implements JoinOperation {

    @Override
    public void run() throws Exception {
        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        if (clusterMetadataManager.getHotRestartStatus() == FORCE_STARTED) {
            Address callerAddress = getCallerAddress();
            getLogger().warning("Notifying member " + callerAddress + " for force start.");
            getNodeEngine().getOperationService().send(new ForceStartMemberOperation(), callerAddress);
        }
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
        return HotRestartClusterSerializerHook.CHECK_IF_MASTER_FORCE_STARTED;
    }

}
