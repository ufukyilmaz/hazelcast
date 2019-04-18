package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.nio.serialization.SerializableByConvention.Reason.PUBLIC_API;
import static com.hazelcast.spi.hotrestart.cluster.PartitionTableReader.readPartitionTable;
import static com.hazelcast.spi.hotrestart.cluster.PartitionTableWriter.writePartitionTable;

/**
 * Contains information about the state read from the disk and the progress of a node during cluster start
 */
@SerializableByConvention(PUBLIC_API)
public class MemberClusterStartInfo implements DataSerializable, Versioned {

    // RU_COMPAT_3_11
    private static final String UNKNOWN_UID = "<unknown-uuid>";

    /**
     * Data load status for eache member during Hot Restart
     */
    public enum DataLoadStatus {
        /**
         * Denotes that member data load is in progress
         */
        LOAD_IN_PROGRESS,

        /**
         * Denotes that member data load is completed successfully
         */
        LOAD_SUCCESSFUL,

        /**
         * Denotes that member data load is failed
         */
        LOAD_FAILED
    }

    private PartitionTableView partitionTable;

    private DataLoadStatus dataLoadStatus;

    public MemberClusterStartInfo() {
    }

    public MemberClusterStartInfo(PartitionTableView partitionTable, DataLoadStatus dataLoadStatus) {
        this.partitionTable = partitionTable;
        this.dataLoadStatus = dataLoadStatus;
    }

    public int getPartitionTableVersion() {
        return partitionTable.getVersion();
    }

    public PartitionTableView getPartitionTable() {
        return partitionTable;
    }

    public DataLoadStatus getDataLoadStatus() {
        return dataLoadStatus;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (out.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            writePartitionTable(out, partitionTable);
        } else {
            // RU_COMPAT_3_11
            writePartitionTableLegacy(partitionTable, out);
        }
        out.writeUTF(dataLoadStatus.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        if (in.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            partitionTable = readPartitionTable(in);
        } else {
            // RU_COMPAT_3_11
            partitionTable = readPartitionTableLegacy(in);
        }
        dataLoadStatus = DataLoadStatus.valueOf(in.readUTF());
    }

    // RU_COMPAT_3_11
    private static void writePartitionTableLegacy(PartitionTableView table, ObjectDataOutput out) throws IOException {
        out.writeInt(table.getLength());
        for (int i = 0; i < table.getLength(); i++) {
            PartitionReplica[] replicas = table.getReplicas(i);
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                PartitionReplica replica = replicas[j];
                boolean addressExists = replica != null;
                out.writeBoolean(addressExists);
                if (addressExists) {
                    replica.address().writeData(out);
                }
            }
        }

        out.writeInt(table.getVersion());
    }

    // RU_COMPAT_3_11
    private static PartitionTableView readPartitionTableLegacy(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        PartitionReplica[][] replicas = new PartitionReplica[len][MAX_REPLICA_COUNT];
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                boolean exists = in.readBoolean();
                if (exists) {
                    Address address = new Address();
                    address.readData(in);
                    replicas[i][j] = new PartitionReplica(address, UNKNOWN_UID);
                }
            }
        }
        int version = in.readInt();
        return new PartitionTableView(replicas, version);
    }
}
