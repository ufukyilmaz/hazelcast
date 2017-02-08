package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.version.Version;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Writes cluster version to a specific file.
 */
class ClusterVersionWriter extends AbstractMetadataWriter<Version> {

    static final String NULL_VERSION = "null";
    static final String FILE_NAME = "cluster-version.txt";

    ClusterVersionWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }

    @Override
    synchronized void doWrite(DataOutput out, Version newVersion) throws IOException {
        out.writeUTF(newVersion == null ? NULL_VERSION : newVersion.toString());
    }
}
