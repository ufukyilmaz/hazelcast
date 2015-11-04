package com.hazelcast.spi.hotrestart.impl;

import java.io.IOException;
import java.io.InputStream;

/**
 * Buffered input stream supporting the minimum needed functionality
 * of {@link java.io.BufferedInputStream} in a streamlined manner.
 */
public class BufferingInputStream extends InputStream {
    /** Base-2 logarithm of buffer size. */
    public static final int LOG_OF_BUFFER_SIZE = 16;
    /** Buffer size used for I/O. Directly affects LZ4 block size,
     * which influences the compression ratio.
     * Invariant: buffer size is a power of two. **/
    public static final int BUFFER_SIZE = 1 << LOG_OF_BUFFER_SIZE;
    static final int BYTE_MASK = 0xff;
    private final InputStream in;
    private final byte[] buf;
    private int position;
    private int limit;

    public BufferingInputStream(InputStream in) {
        this.in = in;
        this.buf = new byte[BUFFER_SIZE];
    }

    @Override public int read() throws IOException {
        if (!ensureDataInBuffer()) {
            return -1;
        }
        return buf[position++] & BYTE_MASK;
    }

    @Override public int read(byte[] destBuf, int off, int len) throws IOException {
        if (!ensureDataInBuffer()) {
            return -1;
        }
        final int transferredCount = Math.min(limit - position, len);
        System.arraycopy(buf, position, destBuf, off, transferredCount);
        position += transferredCount;
        return transferredCount;
    }

    private boolean ensureDataInBuffer() throws IOException {
        if (position != limit) {
            return true;
        }
        position = 0;
        final int newLimit = in.read(buf);
        if (newLimit == -1) {
            limit = 0;
            return false;
        } else {
            limit = newLimit;
            return true;
        }
    }
}
