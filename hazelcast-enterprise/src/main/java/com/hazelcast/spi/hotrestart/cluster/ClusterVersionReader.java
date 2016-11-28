package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.logging.ILogger;
import com.hazelcast.version.ClusterVersion;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.cluster.ClusterVersionWriter.NULL_VERSION;

/**
 * Reads cluster version from a specific file.
 */
class ClusterVersionReader extends AbstractMetadataReader {

    private final ILogger logger;

    private ClusterVersion clusterVersion;

    ClusterVersionReader(ILogger logger, File homeDir) {
        super(homeDir);
        this.logger = logger;
    }

    static ClusterVersion readClusterVersion(ILogger logger, File homeDir) throws IOException {
        final ClusterVersionReader clusterVersionReader = new ClusterVersionReader(logger, homeDir);
        clusterVersionReader.read();
        return clusterVersionReader.clusterVersion;
    }

    @Override
    String getFilename() {
        return ClusterVersionWriter.FILE_NAME;
    }

    @Override
    void doRead(DataInputStream in) throws IOException {
        String name = in.readUTF();
        if (name.equals(NULL_VERSION)) {
            clusterVersion = null;
        } else {
            clusterVersion = ClusterVersion.of(name);
        }
        if (logger.isFineEnabled()) {
            logger.fine("Read cluster version " + clusterVersion + " from disk.");
        }
    }
}
