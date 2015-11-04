package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.hotrestart.HotRestartService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * Operation, which is used to send local partition table to master member
 * during cluster-wide validation phase.
 */
public class SendPartitionTableForValidationOperation
        extends AbstractOperation
        implements JoinOperation {

    private Address[][] partitionTable;

    public SendPartitionTableForValidationOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public SendPartitionTableForValidationOperation(Address[][] partitionTable) {
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
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);

        int len = partitionTable != null ? partitionTable.length : 0;
        out.writeInt(len);
        for (int i = 0; i < len; i++) {
            Address[] replicas = partitionTable[i];
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                Address replica = replicas[j];
                boolean replicaExists = replica != null;
                out.writeBoolean(replicaExists);
                if (replicaExists) {
                    replica.writeData(out);
                }
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);

        int len = in.readInt();
        partitionTable = new Address[len][MAX_REPLICA_COUNT];
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                boolean exists = in.readBoolean();
                if (exists) {
                    Address address = new Address();
                    partitionTable[i][j] = address;
                    address.readData(in);
                }
            }
        }
    }
}
