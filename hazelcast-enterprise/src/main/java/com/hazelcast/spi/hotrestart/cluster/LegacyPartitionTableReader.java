package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads partition table from a specific file if exists.
 */
// RU_COMPAT_3_11
final class LegacyPartitionTableReader extends AbstractMetadataReader {

    private final Address[][] addresses;
    private int partitionVersion;

    LegacyPartitionTableReader(File homeDir, int partitionCount) {
        super(homeDir);
        addresses = new Address[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
    }

    @Override
    void doRead(DataInput in) throws IOException {
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
        return LegacyPartitionTableWriter.FILE_NAME;
    }

    // We don't want to change persistence format, that's why we keep persisting Address[][]
    // and then convert to PartitionReplica[][] using given PartitionReplicas
    PartitionTableView getPartitionTable(PartitionReplica[] replicas) {
        Map<Address, PartitionReplica> mapping = new HashMap<Address, PartitionReplica>(replicas.length);
        for (PartitionReplica member : replicas) {
            mapping.put(member.address(), member);
        }

        return toPartitionTableView(mapping);
    }

    private PartitionTableView toPartitionTableView(Map<Address, PartitionReplica> mapping) {
        PartitionReplica[][] replicas = new PartitionReplica[addresses.length][InternalPartition.MAX_REPLICA_COUNT];
        for (int i = 0; i < replicas.length; i++) {
            for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                replicas[i][j] = mapping.get(addresses[i][j]);
            }
        }
        return new PartitionTableView(replicas, partitionVersion);
    }
}
