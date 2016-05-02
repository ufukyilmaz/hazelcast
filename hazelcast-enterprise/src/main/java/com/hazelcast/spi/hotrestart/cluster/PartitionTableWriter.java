package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Writes partition table to a specific file
 * by overwriting previous one if exists.
 */
class PartitionTableWriter extends AbstractMetadataWriter<InternalPartition[]> {

    static final String FILE_NAME = "partitions.bin";

    private int partitionVersion;

    PartitionTableWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }

    @Override
    void doWrite(DataOutput out, InternalPartition[] partitions) throws IOException {
        out.writeInt(partitions.length);
        out.writeInt(partitionVersion);
        for (InternalPartition partition : partitions) {
            for (int replica = 0; replica < InternalPartition.MAX_REPLICA_COUNT; replica++) {
                final Address address = partition.getReplicaAddress(replica);
                if (address != null) {
                    out.writeBoolean(true);
                    writeAddress(out, address);
                } else {
                    out.writeBoolean(false);
                }
            }
        }
    }

    void setPartitionVersion(int partitionVersion) {
        this.partitionVersion = partitionVersion;
    }
}
