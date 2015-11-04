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
public class SendClusterLoadResultOperation extends AbstractOperation implements JoinOperation {

    private int result;
    private ClusterState loadedState;

    public SendClusterLoadResultOperation() {
    }

    public SendClusterLoadResultOperation(int result, ClusterState loadedState) {
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
        clusterMetadataManager.receiveClusterWideLoadResultFromMaster(result);
        if (result == Result.SUCCESS) {
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
        out.writeInt(result);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        loadedState = ClusterState.valueOf(in.readUTF());
        result = in.readInt();
    }
}
