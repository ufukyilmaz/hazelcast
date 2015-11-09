package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * Reads & writes cluster state to a specific file.
 */
class ClusterStateReaderWriter {
    private static final String FILE_NAME = "cluster.state";

    private final File homeDir;

    private volatile ClusterState clusterState = ClusterState.ACTIVE;

    ClusterStateReaderWriter(File homeDir) {
        this.homeDir = homeDir;
    }

    synchronized void write(ClusterState state) throws IOException {
        File file = new File(homeDir, FILE_NAME);
        FileOutputStream fileOut = null;
        try {
            fileOut = new FileOutputStream(file);
            DataOutputStream out = new DataOutputStream(fileOut);
            out.writeUTF(state.toString());
            closeResource(out);
        } finally {
            closeResource(fileOut);
        }
    }

    void read() throws IOException {
        File file = new File(homeDir, FILE_NAME);
        if (!file.exists()) {
            return;
        }

        FileInputStream fileIn = null;
        try {
            fileIn = new FileInputStream(file);
            DataInputStream in = new DataInputStream(fileIn);
            String name = in.readUTF();
            closeResource(in);
            clusterState = ClusterState.valueOf(name);
        } finally {
            closeResource(fileIn);
        }
    }

    ClusterState get() {
        return clusterState;
    }
}
