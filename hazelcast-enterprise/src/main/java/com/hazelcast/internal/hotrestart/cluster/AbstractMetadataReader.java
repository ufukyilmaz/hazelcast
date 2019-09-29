package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.nio.Address;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

/**
 * Abstract class for reading a single metadata from a specific file
 * if exists.
 */
abstract class AbstractMetadataReader {

    final File homeDir;

    AbstractMetadataReader(File homeDir) {
        this.homeDir = homeDir;
    }

    final void read() throws IOException {
        File file = new File(homeDir, getFilename());
        if (!file.exists()) {
            return;
        }
        DataInputStream in = null;
        try {
            in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            doRead(in);
            in.close();
        } finally {
            closeResource(in);
        }
    }

    abstract String getFilename();

    abstract void doRead(DataInput in) throws IOException;

    static Address readAddress(DataInput in) throws IOException {
        return new Address(in.readUTF(), in.readInt());
    }
}
