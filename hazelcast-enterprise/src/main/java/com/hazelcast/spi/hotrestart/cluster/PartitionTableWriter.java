package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Writes partition table to a specific file
 * by overwriting previous one if exists.
 */
class PartitionTableWriter extends AbstractMetadataWriter<PartitionTableView> {

    static final String FILE_NAME = "partitions.bin";

    PartitionTableWriter(File homeDir) {
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
            for (int replica = 0; replica < InternalPartition.MAX_REPLICA_COUNT; replica++) {
                Address address = partitionTable.getAddress(partition, replica);
                if (address != null) {
                    out.writeBoolean(true);
                    writeAddress(out, address);
                } else {
                    out.writeBoolean(false);
                }
            }
        }
    }
}
