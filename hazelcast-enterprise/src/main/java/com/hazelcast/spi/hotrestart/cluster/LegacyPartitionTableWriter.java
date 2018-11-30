package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Writes partition table to a specific file
 * by overwriting previous one if exists.
 */
// RU_COMPAT_3_11
class LegacyPartitionTableWriter extends AbstractMetadataWriter<PartitionTableView> {

    static final String FILE_NAME = PartitionTableWriter.FILE_NAME;

    LegacyPartitionTableWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }

    @Override
    void doWrite(DataOutput out, PartitionTableView partitionTable) throws IOException {
        out.writeInt(partitionTable.getVersion());
        int length = partitionTable.getLength();
        out.writeInt(length);
        for (int partition = 0; partition < length; partition++) {
            for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                PartitionReplica replica = partitionTable.getReplica(partition, index);
                if (replica != null) {
                    out.writeBoolean(true);
                    writeAddress(out, replica.address());
                } else {
                    out.writeBoolean(false);
                }
            }
        }
    }
}
