package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * Writes partition table to a specific file
 * by overwriting previous one if exists.
 *
 * @since 4.1
 */
final class PartitionTableWriter extends LegacyPartitionTableWriter {

    PartitionTableWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    void doWrite(DataOutput out, PartitionTableView partitionTable) throws IOException {
        writePartitionTable(out, partitionTable);
    }

    static void writePartitionTable(DataOutput out, PartitionTableView partitionTable) throws IOException {
        out.writeInt(partitionTable.length());

        LinkedHashMap<PartitionReplica, Integer> replicaIdToIndexes = createReplicaIdToIndexMap(partitionTable);
        writeAllReplicas(out, replicaIdToIndexes.keySet());

        writePartitionTable(partitionTable, replicaIdToIndexes, out);
    }

    private static void writePartitionTable(PartitionTableView partitionTable, Map<PartitionReplica, Integer> replicaIdToIndexes,
            DataOutput out) throws IOException {
        for (int partitionId = 0; partitionId < partitionTable.length(); partitionId++) {
            InternalPartition partition = partitionTable.getPartition(partitionId);
            out.writeInt(partition.version());
            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                PartitionReplica replica = partition.getReplica(replicaIndex);
                if (replica == null) {
                    out.writeInt(-1);
                } else {
                    int index = replicaIdToIndexes.get(replica);
                    out.writeInt(index);
                }
            }
        }
    }
}
