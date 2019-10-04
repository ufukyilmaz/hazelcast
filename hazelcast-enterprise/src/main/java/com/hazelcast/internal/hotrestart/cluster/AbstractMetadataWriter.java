package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.Address;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.rename;

/**
 * Abstract class for writing a single metadata to a specific file
 * by overwriting existing file if exists.
 *
 * @param <T> parameter type to be written
 */
abstract class AbstractMetadataWriter<T> {

    private static final String TMP_SUFFIX = ".tmp";

    private final File homeDir;

    AbstractMetadataWriter(File homeDir) {
        this.homeDir = homeDir;
    }

    // this will be called by the class's client
    final synchronized void write(T param) throws IOException {
        final File tmpFile = new File(homeDir, getFilename() + TMP_SUFFIX);
        final FileOutputStream fileOut = new FileOutputStream(tmpFile);
        final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOut));
        try {
            doWrite(out, param);
            out.flush();
            fileOut.getFD().sync();
            out.close();
            rename(tmpFile, new File(homeDir, getFilename()));
        } finally {
            closeResource(out);
        }
    }

    abstract String getFilename();

    abstract void doWrite(DataOutput out, T param) throws IOException;

    // this will be called from the subclasses
    static void writeAddress(DataOutput out, Address address) throws IOException {
        out.writeUTF(address.getHost());
        out.writeInt(address.getPort());
    }
}
