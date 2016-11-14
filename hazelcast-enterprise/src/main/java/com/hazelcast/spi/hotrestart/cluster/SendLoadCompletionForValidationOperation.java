package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.hotrestart.HotRestartService;

import java.io.IOException;

/**
 * Operation, which is used to send local Hot Restart load completion result (success or failure)
 * to master member after load phase.
 */
public class SendLoadCompletionForValidationOperation
        extends SendPartitionTableForValidationOperation implements JoinOperation {

    private boolean success;

    public SendLoadCompletionForValidationOperation() {
    }

    public SendLoadCompletionForValidationOperation(PartitionTableView partitionTable, boolean success) {
        super(partitionTable);
        this.success = success;
    }

    @Override
    public void run() throws Exception {
        super.run();

        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receiveLoadCompletionStatusFromMember(getCallerAddress(), success);
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

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartClusterSerializerHook.SEND_LOAD_COMPLETION_FOR_VALIDATION;
    }
}
