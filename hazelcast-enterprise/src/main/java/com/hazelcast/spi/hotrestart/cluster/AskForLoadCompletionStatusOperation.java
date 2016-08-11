package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.hotrestart.HotRestartService;

import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PENDING_VERIFICATION;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_FAILED;

/**
 * It may happen that master can fail just after setting final status of the cluster start. If next master doesn't receive
 * the status change and some of other nodes receive, next master can not complete start process. Master sends this operation to
 * other nodes to check if there is a status change it missed.
 */
public class AskForLoadCompletionStatusOperation extends AbstractOperation implements JoinOperation {

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Address caller = getCallerAddress();
        ILogger logger = getLogger();
        final Address master = nodeEngine.getMasterAddress();
        if (!master.equals(caller)) {
            logger.warning("Non-master member: " + caller + " asked for load completion status. master: " + master);
            return;
        }

        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        OperationService operationService = nodeEngine.getOperationService();

        HotRestartClusterInitializationStatus status = clusterMetadataManager.getHotRestartStatus();
        if (status == VERIFICATION_AND_LOAD_SUCCEEDED || status == VERIFICATION_FAILED) {
            final ClusterState clusterState = clusterMetadataManager.getCurrentClusterState();
            if (logger.isFineEnabled()) {
                logger.fine("Sending hot restart status: " + status + " and cluster state: " + clusterState + " to: " + caller
                        + " as response for " + getClass().getSimpleName());
            }
            operationService.send(new SendLoadCompletionStatusOperation(status, clusterState), caller);
        } else if (status == FORCE_STARTED) {
            if (logger.isFineEnabled()) {
                logger.fine("Sending force start to: " + caller + " as response for " + getClass().getSimpleName());
            }
            operationService.send(new ForceStartMemberOperation(), caller);
        } else if (status == PENDING_VERIFICATION) {
            logger.warning("Should not receive " + getClass().getSimpleName() + " from member: " + caller
                    + " while hot restart status is " + PENDING_VERIFICATION);
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

}
