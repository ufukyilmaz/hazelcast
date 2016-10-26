package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

/**
 * Reads partition table from a specific file if exists.
 */
final class PartitionTableReader extends AbstractMetadataReader {

    private final Address[][] addresses;
    private int partitionVersion;

    PartitionTableReader(File homeDir, int partitionCount) {
        super(homeDir);
        addresses = new Address[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
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
        if (partitionCount != addresses.length) {
            throw new IOException("Invalid partition count! Expected: " + addresses.length + ", Actual: " + partitionCount);
        }

        try {
            for (int partition = 0; partition < addresses.length; partition++) {
                for (int replica = 0; replica < InternalPartition.MAX_REPLICA_COUNT; replica++) {
                    addresses[partition][replica] = in.readBoolean() ? readAddress(in) : null;
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

    PartitionTableView getTable() {
        return new PartitionTableView(addresses, partitionVersion);
    }
}
