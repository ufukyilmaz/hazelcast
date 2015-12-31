package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.impl.operations.JoinOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.hotrestart.HotRestartService;

import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.FORCE_STARTED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.PARTITION_TABLE_VERIFIED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED;
import static com.hazelcast.spi.hotrestart.cluster.HotRestartClusterInitializationStatus.VERIFICATION_FAILED;

/**
 * It may happen that master can fail just after validating partition table of the nodes. If next master doesn't receive
 * the status change and some of other nodes receive, next master can not validate partition tables since some of other nodes
 * will not send their partition table as they started loading hot restart data. Master sends this operation to other nodes to
 * check if there is a status change it missed.
 */
public class AskForPartitionTableValidationStatusOperation extends AbstractOperation implements JoinOperation  {

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Address caller = getCallerAddress();
        ILogger logger = getLogger();
        if (!nodeEngine.getMasterAddress().equals(caller)) {
            logger.warning("Received " + getClass().getSimpleName() + " from non-master member: " + caller);
            return;
        }

        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        OperationService operationService = nodeEngine.getOperationService();

        HotRestartClusterInitializationStatus status = clusterMetadataManager.getHotRestartStatus();
        if (status == PARTITION_TABLE_VERIFIED || status == VERIFICATION_FAILED) {
            if (logger.isFineEnabled()) {
                logger.fine("Sending " + status + " to: " + caller + " as response for " + getClass().getSimpleName());
            }
            operationService.send(new SendPartitionTableValidationResultOperation(status), caller);
        } else if (status == FORCE_STARTED) {
            if (logger.isFineEnabled()) {
                logger.fine("Sending force start to: " + caller + " as response for " + getClass().getSimpleName());
            }
            operationService.send(new ForceStartMemberOperation(), caller);
        } else if (status == VERIFICATION_AND_LOAD_SUCCEEDED) {
            logger.warning("Should not receive " + getClass().getSimpleName() + " from member: " + caller
                    + " while hot restart status is " + VERIFICATION_AND_LOAD_SUCCEEDED);
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
