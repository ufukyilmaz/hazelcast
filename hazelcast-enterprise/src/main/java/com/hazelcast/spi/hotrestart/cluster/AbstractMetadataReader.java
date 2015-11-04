package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.StringUtil.UTF8_CHARSET;

/**
 * Abstract class for reading a single metadata from a specific file
 * if exists.
 */
abstract class AbstractMetadataReader {

    private final File homeDir;

    AbstractMetadataReader(File homeDir) {
        this.homeDir = homeDir;
    }

    final void read() throws IOException {
        File file = new File(homeDir, getFileName());
        if (!file.exists()) {
            return;
        }

        FileInputStream fileIn = null;
        try {
            fileIn = new FileInputStream(file);
            DataInputStream in = new DataInputStream(new BufferedInputStream(fileIn));

            doRead(in);

            closeResource(in);
        } catch (IOException e) {
            throw new IOException("Cannot read partition table from: " + file.getAbsolutePath(), e);
        } finally {
            closeResource(fileIn);
        }
    }

    protected abstract void doRead(DataInputStream in) throws IOException;

    protected abstract String getFileName();

    static Address readAddressFromStream(DataInput in) throws IOException {
        int hostLen = in.readInt();
        byte[] hostBytes = new byte[hostLen];
        in.readFully(hostBytes);
        String host = new String(hostBytes, 0, hostLen, UTF8_CHARSET);
        int port = in.readInt();

        return new Address(host, port);
    }
}
