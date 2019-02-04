package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.version.Version;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.cluster.ClusterVersionWriter.UNKNOWN_VERSION;

/**
 * Reads cluster version from a specific file.
 */
class ClusterVersionReader extends AbstractMetadataReader {

    private Version clusterVersion = Version.UNKNOWN;

    ClusterVersionReader(File homeDir) {
        super(homeDir);
    }

    static Version readClusterVersion(File homeDir) throws IOException {
        final ClusterVersionReader clusterVersionReader = new ClusterVersionReader(homeDir);
        clusterVersionReader.read();
        return clusterVersionReader.clusterVersion;
    }

    @Override
    String getFilename() {
        return ClusterVersionWriter.FILE_NAME;
    }

    @Override
    void doRead(DataInput in) throws IOException {
        String name = in.readUTF();
        if (!name.equals(UNKNOWN_VERSION)) {
            clusterVersion = Version.of(name);
        }
    }
}
