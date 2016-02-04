package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.hotrestart.HotRestartService;

import java.io.IOException;

/**
 * Operation, which is used to send cluster-wide validation result (success or failure)
 * by master member to all cluster after cluster-wide validation phase completes.
 */
public class SendPartitionTableValidationResultOperation
        extends AbstractOperation implements JoinOperation {

    private HotRestartClusterInitializationStatus result;

    public SendPartitionTableValidationResultOperation() {
    }

    public SendPartitionTableValidationResultOperation(HotRestartClusterInitializationStatus result) {
        this.result = result;
    }

    @Override
    public void run() throws Exception {
        Address caller = getCallerAddress();
        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveHotRestartStatusFromMasterAfterPartitionTableVerification(caller, result);
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
        out.writeUTF(result.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        result = HotRestartClusterInitializationStatus.valueOf(in.readUTF());
    }
}
