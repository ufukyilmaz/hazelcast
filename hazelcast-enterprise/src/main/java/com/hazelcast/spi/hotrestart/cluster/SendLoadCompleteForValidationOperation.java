package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.hotrestart.HotRestartService;

import java.io.IOException;

/**
 * Operation, which is used to send local hot-restart load completion result (success or failure)
 * to master member after load phase.
 */
public class SendLoadCompleteForValidationOperation
        extends AbstractOperation implements JoinOperation {

    private boolean success;

    public SendLoadCompleteForValidationOperation() {
    }

    public SendLoadCompleteForValidationOperation(boolean success) {

        this.success = success;
    }

    @Override
    public void run() throws Exception {
        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveLoadCompleteFromMember(getCallerAddress(), success);
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
        out.writeBoolean(success);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        success = in.readBoolean();
    }
}
