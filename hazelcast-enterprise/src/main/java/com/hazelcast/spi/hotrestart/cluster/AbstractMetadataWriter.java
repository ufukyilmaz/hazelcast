package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Address;
import com.hazelcast.util.StringUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Abstract class for writing a single metadata to a specific file
 * by overwriting existing file if exists.
 */
abstract class AbstractMetadataWriter<T> {

    static final int DEFAULT_BUFFER_SIZE = (int) MemoryUnit.KILOBYTES.toBytes(32);

    private final File homeDir;
    private final ByteBuffer buffer;
    private FileOutputStream out;

    AbstractMetadataWriter(File homeDir) {
        this(homeDir, DEFAULT_BUFFER_SIZE);
    }

    AbstractMetadataWriter(File homeDir, int bufferSize) {
        checkPositive(bufferSize, "Buffer size should be positive!");
        this.homeDir = homeDir;
        buffer = ByteBuffer.allocate(bufferSize);
    }

    final synchronized void write(T param) throws IOException {
        File file = new File(homeDir, getNewFileName());
        try {
            out = new FileOutputStream(file, false);

            doWrite(param);

            writeBufferToFile();

            closeResource(out);

            if (!file.renameTo(new File(homeDir, getFileName()))) {
                throw new IOException("Failed to rename " + file.getAbsolutePath());
            }
        } finally {
            buffer.clear();
            closeResource(out);
        }
    }

    final void writeByte(byte b) throws IOException {
        flushBufferIfRequired(1);
        buffer.put(b);
    }

    final void writeInt(int i) throws IOException {
        flushBufferIfRequired(INT_SIZE_IN_BYTES);
        buffer.putInt(i);
    }

    private synchronized void flushBufferIfRequired(int requiredSpace) throws IOException {
        if (buffer.remaining() < requiredSpace) {
            writeBufferToFile();
        }
    }

    synchronized final void writeAddress(Address address) throws IOException {
        if (!writeAddressToBuffer(address, buffer)) {
            writeBufferToFile();

            if (!writeAddressToBuffer(address, buffer)) {
                throw new IOException("Not expected! " + address + " is not fitting into " + buffer);
            }
        }
    }

    private void writeBufferToFile() throws IOException {
        buffer.flip();
        if (!buffer.hasRemaining()) {
            return;
        }
        byte[] bytes = buffer.array();
        out.write(bytes, 0, buffer.remaining());
        buffer.clear();
    }

    abstract void doWrite(T param) throws IOException;

    abstract String getFileName();

    abstract String getNewFileName();

    private static boolean writeAddressToBuffer(Address address, ByteBuffer buffer) {
        byte[] host = StringUtil.stringToBytes(address.getHost());
        int port = address.getPort();
        int requiredLen = host.length + INT_SIZE_IN_BYTES * 2;

        if (buffer.remaining() < requiredLen) {
            return false;
        }

        buffer.putInt(host.length);
        buffer.put(host);
        buffer.putInt(port);
        return true;
    }
}
