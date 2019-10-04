package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.cluster.Address;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * Reads partition table from a specific file if exists.
 *
 * @since 3.12
 */
final class PartitionTableReader extends AbstractMetadataReader {

    private final int partitionCount;
    private PartitionTableView partitionTable;

    PartitionTableReader(File homeDir, int partitionCount) {
        super(homeDir);
        this.partitionCount = partitionCount;
        this.partitionTable = new PartitionTableView(new PartitionReplica[partitionCount][MAX_REPLICA_COUNT], 0);
    }

    @Override
    void doRead(DataInput in) throws IOException {
        PartitionTableView pt = readPartitionTable(in);
        if (pt.getLength() != partitionCount) {
            throw new IOException("Invalid partition count! Expected: " + partitionCount
                    + ", Actual: " + pt.getLength());
        }
        partitionTable = pt;
    }

    static PartitionTableView readPartitionTable(DataInput in) throws IOException {
        int partitionVersion;
        try {
            partitionVersion = in.readInt();
        } catch (IOException e) {
            throw new IOException("Cannot read partition version!", e);
        }

        int partitionCount;
        try {
            partitionCount = in.readInt();
        } catch (IOException e) {
            throw new IOException("Cannot read partition count!", e);
        }

        PartitionReplica[] replicas;
        try {
            int len = in.readInt();
            replicas = new PartitionReplica[len];
            for (int i = 0; i < len; i++) {
                Address address = readAddress(in);
                UUID uuid = UUIDSerializationUtil.readUUID(in);
                replicas[i] = new PartitionReplica(address, uuid);
            }
        } catch (IOException e) {
            throw new IOException("Cannot read partition table replicas!", e);
        }

        PartitionReplica[][] table = new PartitionReplica[partitionCount][MAX_REPLICA_COUNT];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                int index = in.readInt();
                if (index != -1) {
                    PartitionReplica member = replicas[index];
                    assert member != null;
                    table[partitionId][replicaIndex] = member;
                }
            }
        }
        return new PartitionTableView(table, partitionVersion);
    }

    @Override
    String getFilename() {
        return PartitionTableWriter.FILE_NAME;
    }

    PartitionTableView getPartitionTable() {
        return partitionTable;
    }
}
