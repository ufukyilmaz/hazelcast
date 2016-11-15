package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.Member;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

/**
 * Manages the initiation of WAN sync requests
 */
public class WanSyncManager {

    private final ClusterService clusterService;
    private final OperationService operationService;
    private final ILogger logger;


    public WanSyncManager(NodeEngine nodeEngine) {
        clusterService = nodeEngine.getClusterService();
        operationService = nodeEngine.getOperationService();
        logger = nodeEngine.getLogger(getClass());
    }

    public void initiateSyncMapRequest(String wanReplicationName, String targetGroupName, String mapName) {
        for (Member member : clusterService.getMembers()) {
            Operation operation = new WanSyncOperation(wanReplicationName, targetGroupName,
                    new WanSyncEvent(WanSyncType.SINGLE_MAP, mapName));
            Future future = operationService.invokeOnTarget(EnterpriseWanReplicationService.SERVICE_NAME,
                    operation, member.getAddress());
            try {
                future.get();
            } catch (Exception ex) {
                logger.warning("Exception occurred while initiating sync map request", ex);
                ExceptionUtil.rethrow(ex);
            }
        }
    }
}
