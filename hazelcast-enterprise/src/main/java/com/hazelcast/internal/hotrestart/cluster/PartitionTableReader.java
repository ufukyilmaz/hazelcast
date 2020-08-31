package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.util.UUIDSerializationUtil;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * Reads partition table from a specific file if exists.
 *
 * @since 4.1
 */
class PartitionTableReader extends AbstractMetadataReader {

    private final int partitionCount;
    private PartitionTableView partitionTable;

    PartitionTableReader(File homeDir, int partitionCount) {
        super(homeDir);
        this.partitionCount = partitionCount;
        InternalPartition[] partitions = new InternalPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitions[i] = new ReadonlyInternalPartition(new PartitionReplica[MAX_REPLICA_COUNT], i, 0);
        }
        this.partitionTable = new PartitionTableView(partitions);
    }

    @Override
    final void doRead(DataInput in) throws IOException {
        PartitionTableView pt = readPartitionTable(in);
        if (pt.length() != partitionCount) {
            throw new IOException("Invalid partition count! Expected: " + partitionCount
                    + ", Actual: " + pt.length());
        }
        partitionTable = pt;
    }

    static PartitionTableView readPartitionTable(DataInput in) throws IOException {
        int partitionCount;
        try {
            partitionCount = in.readInt();
        } catch (IOException e) {
            throw new IOException("Cannot read partition count!", e);
        }

        PartitionReplica[] allReplicas;
        try {
            int len = in.readInt();
            allReplicas = new PartitionReplica[len];
            for (int i = 0; i < len; i++) {
                Address address = readAddress(in);
                UUID uuid = UUIDSerializationUtil.readUUID(in);
                allReplicas[i] = new PartitionReplica(address, uuid);
            }
        } catch (IOException e) {
            throw new IOException("Cannot read partition table replicas!", e);
        }

        InternalPartition[] partitions = new InternalPartition[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            int version = in.readInt();
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                int index = in.readInt();
                if (index != -1) {
                    PartitionReplica replica = allReplicas[index];
                    assert replica != null;
                    replicas[replicaIndex] = replica;
                }
            }
            partitions[partitionId] = new ReadonlyInternalPartition(replicas, partitionId, version);
        }

        return new PartitionTableView(partitions);
    }

    @Override
    String getFilename() {
        return PartitionTableWriter.FILE_NAME;
    }

    PartitionTableView getPartitionTable() {
        return partitionTable;
    }
}
