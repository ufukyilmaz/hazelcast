package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Reads partition table from a specific file if exists.
 */
final class PartitionTableReader extends AbstractMetadataReader {

    private final Address[][] table;
    private int partitionVersion;

    PartitionTableReader(File homeDir, int partitionCount) {
        super(homeDir);
        table = new Address[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
        int partitionCount = in.readInt();
        if (partitionCount != table.length) {
            throw new IOException("Invalid partition count! Expected: " + table.length + ", Actual: " + partitionCount);
        }
        try {
            partitionVersion = in.readInt();
        } catch (IOException e) {
            throw new IOException("Cannot read partition version!", e);
        }
        try {
            for (int partition = 0; partition < table.length; partition++) {
                for (int replica = 0; replica < InternalPartition.MAX_REPLICA_COUNT; replica++) {
                    table[partition][replica] = in.readBoolean() ? readAddress(in) : null;
                }
            }
        } catch (IOException e) {
            throw new IOException("Cannot read partition table!", e);
        }
    }

    @Override
    String getFilename() {
        return PartitionTableWriter.FILE_NAME;
    }

    int getPartitionVersion() {
        return partitionVersion;
    }

    Address[][] getTable() {
        return table;
    }
}
