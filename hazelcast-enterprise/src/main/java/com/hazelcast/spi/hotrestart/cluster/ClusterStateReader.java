package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;

/**
 * Reads cluster state from a specific file.
 */
class ClusterStateReader extends AbstractMetadataReader {

    private volatile ClusterState clusterState = ClusterState.ACTIVE;

    ClusterStateReader(File homeDir) {
        super(homeDir);
    }

    static ClusterState readClusterState(File homeDir) throws IOException {
        final ClusterStateReader clusterStateReader = new ClusterStateReader(homeDir);
        clusterStateReader.read();
        return clusterStateReader.clusterState;
    }

    @Override
    String getFilename() {
        return ClusterStateWriter.FILE_NAME;
    }

    @Override
    void doRead(DataInput in) throws IOException {
        String name = in.readUTF();
        clusterState = ClusterState.valueOf(name);
    }
}
