package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;

import java.io.File;
import java.io.IOException;

/**
 * Writes partition table to a specific file
 * by overwriting previous one if exists.
 */
class PartitionTableWriter extends AbstractMetadataWriter<InternalPartition[]> {

    static final String FILE_NAME = "partition.data";
    private static final String FILE_NAME_TMP = FILE_NAME + ".tmp";

    PartitionTableWriter(File homeDir) {
        super(homeDir);
    }

    void doWrite(InternalPartition[] partitions) throws IOException {
        for (InternalPartition partition : partitions) {
            for (int replica = 0; replica < InternalPartition.MAX_REPLICA_COUNT; replica++) {
                Address address = partition.getReplicaAddress(replica);
                boolean hasReplica = address != null;
                writeByte((byte) (hasReplica ? 1 : 0));

                if (hasReplica) {
                    writeAddress(address);
                }
            }
        }
    }

    @Override
    String getFileName() {
        return FILE_NAME;
    }

    @Override
    String getNewFileName() {
        return FILE_NAME_TMP;
    }
}
