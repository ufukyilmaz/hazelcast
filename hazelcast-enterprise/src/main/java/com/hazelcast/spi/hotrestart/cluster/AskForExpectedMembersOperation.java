package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Checks the master if this member is excluded in the final cluster start result,
 * during this member is waiting for all members to join
 */
public class AskForExpectedMembersOperation extends Operation implements JoinOperation {

    @Override
    public void run() throws Exception {
        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.replyExpectedMembersQuestion(getCallerAddress(), getCallerUuid());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return HotRestartIntegrationService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HotRestartClusterSerializerHook.ASK_FOR_EXPECTED_MEMBERS;
    }

}
