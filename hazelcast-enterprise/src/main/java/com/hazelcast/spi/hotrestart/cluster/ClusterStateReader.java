package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.logging.ILogger;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;

/**
 * Reads cluster state from a specific file.
 */
class ClusterStateReader extends AbstractMetadataReader {

    private final ILogger logger;

    private volatile ClusterState clusterState = ClusterState.ACTIVE;

    ClusterStateReader(ILogger logger, File homeDir) {
        super(homeDir);
        this.logger = logger;
    }

    static ClusterState readClusterState(ILogger logger, File homeDir) throws IOException {
        final ClusterStateReader clusterStateReader = new ClusterStateReader(logger, homeDir);
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
        if (logger.isFineEnabled()) {
            logger.fine("Read " + clusterState + " from disk.");
        }
    }
}
