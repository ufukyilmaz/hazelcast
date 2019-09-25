package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Set;
import java.util.UUID;

import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_IN_PROGRESS;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterStartStatus.CLUSTER_START_SUCCEEDED;

/**
 * It may happen that master can fail just after validating partition table of the nodes. If next master doesn't receive
 * the status change and some of other nodes receive, next master can not validate partition tables since some of other nodes
 * will not send their partition table as they started loading hot restart data. Master sends this operation to other nodes to
 * check if there is a status change it missed.
 */
public class AskForClusterStartResultOperation extends Operation implements JoinOperation {

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Address caller = getCallerAddress();
        ILogger logger = getLogger();
        Address master = nodeEngine.getMasterAddress();
        if (master == null) {
            logger.warning("Master is not known yet. Cannot answer for cluster start result. Caller: " + caller);
            return;
        }
        if (!master.equals(caller)) {
            logger.warning("Non-master member: " + caller + " asked for cluster start result. master: " + master);
            return;
        }

        HotRestartIntegrationService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        OperationService operationService = nodeEngine.getOperationService();

        HotRestartClusterStartStatus status = clusterMetadataManager.getHotRestartStatus();
        Set<UUID> excludedMemberUuids = clusterMetadataManager.getExcludedMemberUuids();
        if (clusterMetadataManager.isStartWithHotRestart() && status != CLUSTER_START_IN_PROGRESS) {
            if (logger.isFineEnabled()) {
                logger.fine("Sending " + status + " to: " + caller + " as response for " + getClass().getSimpleName());
            }
            ClusterState clusterState = (status == CLUSTER_START_SUCCEEDED)
                    ? clusterMetadataManager.getCurrentClusterState() : null;
            operationService.send(new SendClusterStartResultOperation(status, excludedMemberUuids, clusterState), caller);
        }
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
        return HotRestartClusterSerializerHook.ASK_FOR_CLUSTER_START_RESULT;
    }
}
