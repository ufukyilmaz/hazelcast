package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * Contains information about the state read from the disk and the progress of a node during cluster start
 */
@SerializableByConvention(PUBLIC_API)
public class MemberClusterStartInfo implements DataSerializable, Versioned {

    /**
     * Data load status for each member during Hot Restart
     */
    public enum DataLoadStatus {
        /**
         * Denotes that member data load is in progress
         */
        LOAD_IN_PROGRESS,

        /**
         * Denotes that member data load is completed successfully
         */
        LOAD_SUCCESSFUL,

        /**
         * Denotes that member data load is failed
         */
        LOAD_FAILED
    }

    private PartitionTableView partitionTable;

    private DataLoadStatus dataLoadStatus;

    public MemberClusterStartInfo() {
    }

    public MemberClusterStartInfo(PartitionTableView partitionTable, DataLoadStatus dataLoadStatus) {
        this.partitionTable = partitionTable;
        this.dataLoadStatus = dataLoadStatus;
    }

    public PartitionTableView getPartitionTable() {
        return partitionTable;
    }

    public DataLoadStatus getDataLoadStatus() {
        return dataLoadStatus;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        PartitionTableWriter.writePartitionTable(out, partitionTable);
        out.writeUTF(dataLoadStatus.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionTable = PartitionTableReader.readPartitionTable(in);
        dataLoadStatus = DataLoadStatus.valueOf(in.readUTF());
    }
}
