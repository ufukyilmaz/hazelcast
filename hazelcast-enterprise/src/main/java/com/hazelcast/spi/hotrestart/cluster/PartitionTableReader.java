package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.internal.partition.InternalPartition;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Reads partition table from a specific file if exists.
 */
class PartitionTableReader extends AbstractMetadataReader {
    private static final String FILE_NAME = PartitionTableWriter.FILE_NAME;

    private final Address[][] table;
    private int partitionVersion;

    PartitionTableReader(File homeDir, int partitionCount) {
        super(homeDir);
        table = new Address[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
    }

    @Override
    protected void doRead(DataInputStream in) throws IOException {
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
                    Address address = null;
                    boolean hasReplica = in.readBoolean();
                    if (hasReplica) {
                        address = readAddressFromStream(in);
                    }
                    table[partition][replica] = address;
                }
            }
        } catch (IOException e) {
            throw new IOException("Cannot read partition table!", e);
        }
    }

    @Override
    protected String getFileName() {
        return FILE_NAME;
    }

    int getPartitionVersion() {
        return partitionVersion;
    }

    Address[][] getTable() {
        return table;
    }
}
