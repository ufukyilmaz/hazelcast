package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Writes cluster state to a specific file.
 */
class ClusterStateWriter extends AbstractMetadataWriter<ClusterState> {

    static final String FILE_NAME = "cluster-state.txt";

    ClusterStateWriter(File homeDir) {
        super(homeDir);
    }

    @Override
    String getFilename() {
        return FILE_NAME;
    }

    @Override
    synchronized void doWrite(DataOutput out, ClusterState state) throws IOException {
        out.writeUTF(state.name());
    }
}
