package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Contains information about the state read from the disk and the progress of a node during cluster start
 */
public class MemberClusterStartInfo implements DataSerializable {

    /**
     * Data load status for eache member during Hot Restart
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

    public int getPartitionTableVersion() {
        return partitionTable.getVersion();
    }

    public PartitionTableView getPartitionTable() {
        return partitionTable;
    }

    public DataLoadStatus getDataLoadStatus() {
        return dataLoadStatus;
    }

    public boolean checkPartitionTables(MemberClusterStartInfo other) {
        return partitionTable.equals(other.partitionTable);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        PartitionTableView.writeData(partitionTable, out);
        out.writeUTF(dataLoadStatus.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionTable = PartitionTableView.readData(in);
        dataLoadStatus = DataLoadStatus.valueOf(in.readUTF());
    }

}
