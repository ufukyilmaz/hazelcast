package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.HotRestartService;

import java.io.IOException;

/**
 * Operation, which is used to send cluster-wide completion result (success or failure)
 * by master member to all cluster after cluster-wide load completes.
 */
public class SendLoadCompletionStatusOperation
        extends AbstractOperation implements JoinOperation {

    private HotRestartClusterInitializationStatus result;
    private ClusterState loadedState;

    public SendLoadCompletionStatusOperation() {
    }

    public SendLoadCompletionStatusOperation(HotRestartClusterInitializationStatus result, ClusterState loadedState) {
        this.result = result;
        this.loadedState = loadedState;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Address caller = getCallerAddress();
        if (!nodeEngine.getMasterAddress().equals(caller)) {
            getLogger().warning("Received hot-restart validation result from non-master member: " + caller);
            return;
        }

        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveHotRestartStatusFromMasterAfterLoadCompletion(result);
        if (result == HotRestartClusterInitializationStatus.VERIFICATION_AND_LOAD_SUCCEEDED) {
            clusterMetadataManager.setFinalClusterState(loadedState);
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(loadedState.toString());
        out.writeUTF(result.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        loadedState = ClusterState.valueOf(in.readUTF());
        result = HotRestartClusterInitializationStatus.valueOf(in.readUTF());
    }
}
