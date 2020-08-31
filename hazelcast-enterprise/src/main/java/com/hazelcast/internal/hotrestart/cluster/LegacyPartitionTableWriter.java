package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.util.UUIDSerializationUtil;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * Writes partition table to a specific file
 * by overwriting previous one if exists.
 *
 * @since 3.12
 */
//RU_COMPAT_4_0
@Deprecated
class LegacyPartitionTableWriter extends AbstractMetadataWriter<PartitionTableView> {

    static final String FILE_NAME = "partitions.bin";

    LegacyPartitionTableWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    void doWrite(DataOutput out, PartitionTableView partitionTable) throws IOException {
        writePartitionTable(out, partitionTable);
    }

    static void writePartitionTable(DataOutput out, PartitionTableView partitionTable) throws IOException {
        out.writeInt(partitionTable.version());
        out.writeInt(partitionTable.length());

        LinkedHashMap<PartitionReplica, Integer> replicaIdToIndexes = createReplicaIdToIndexMap(partitionTable);
        writeAllReplicas(out, replicaIdToIndexes.keySet());

        writePartitionTable(partitionTable, replicaIdToIndexes, out);
    }

    static void writeAllReplicas(DataOutput out, Set<PartitionReplica> replicas) throws IOException {
        // replicas is ordered, that's why we can write keys in iteration (same as insertion) order
        out.writeInt(replicas.size());
        for (PartitionReplica replica : replicas) {
            writeAddress(out, replica.address());
            UUIDSerializationUtil.writeUUID(out, replica.uuid());
        }
    }

    private static void writePartitionTable(PartitionTableView partitionTable, Map<PartitionReplica, Integer> replicaIdToIndexes,
            DataOutput out) throws IOException {
        for (int partitionId = 0; partitionId < partitionTable.length(); partitionId++) {
            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                PartitionReplica replica = partitionTable.getReplica(partitionId, replicaIndex);
                if (replica == null) {
                    out.writeInt(-1);
                } else {
                    int index = replicaIdToIndexes.get(replica);
                    out.writeInt(index);
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:illegaltype")
    // Returns a LinkedHashMap with insertion order on purpose.
    static LinkedHashMap<PartitionReplica, Integer> createReplicaIdToIndexMap(PartitionTableView partitionTable) {
        LinkedHashMap<PartitionReplica, Integer> map = new LinkedHashMap<>();
        int addressIndex = 0;
        for (int partitionId = 0; partitionId < partitionTable.length(); partitionId++) {
            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                PartitionReplica replica = partitionTable.getReplica(partitionId, replicaIndex);
                if (replica == null) {
                    continue;
                }
                if (map.containsKey(replica)) {
                    continue;
                }
                map.put(replica, addressIndex++);
            }
        }
        return map;
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }
}
