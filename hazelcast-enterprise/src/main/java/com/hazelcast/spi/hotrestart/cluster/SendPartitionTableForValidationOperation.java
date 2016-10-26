package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Operation, which is used to send local partition table to master member
 * during cluster-wide validation phase.
 */
public class SendPartitionTableForValidationOperation
        extends Operation
        implements JoinOperation {

    private PartitionTableView partitionTable;

    public SendPartitionTableForValidationOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public SendPartitionTableForValidationOperation(PartitionTableView partitionTable) {
        this.partitionTable = partitionTable;
    }

    @Override
    public void run()
            throws Exception {
        HotRestartService service = getService();
        ClusterMetadataManager clusterMetadataManager = service.getClusterMetadataManager();
        clusterMetadataManager.receivePartitionTableFromMember(getCallerAddress(), partitionTable);
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
        PartitionTableView.writeData(partitionTable, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        partitionTable = PartitionTableView.readData(in);
    }

    @Override
    public int getFactoryId() {
        return HotRestartClusterSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HotRestartClusterSerializerHook.SEND_PARTITION_TABLE_FOR_VALIDATION;
    }
}
