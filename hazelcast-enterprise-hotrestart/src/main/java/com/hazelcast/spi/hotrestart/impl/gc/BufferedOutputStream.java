package com.hazelcast.spi.hotrestart.impl.gc;

import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.impl.BufferingInputStream.BUFFER_SIZE;

/**
 * A simple buffered implementation of {@link OutputStream}. Introduced because
 * the JDK's standard implementation suffers from complicated defensive coding,
 * hurting performance.
 */
final class BufferedOutputStream extends FilterOutputStream {
    private final byte[] buf = new byte[BUFFER_SIZE];
    private int position;

    public BufferedOutputStream(FileOutputStream fileOut) {
        super(fileOut);
    }

    @Override public void write(int b) throws IOException {
        buf[position++] = (byte) b;
        ensureBufHasRoom();
    }

    @Override public void write(byte[] b, int off, int len) throws IOException {
        while (len > BUFFER_SIZE) {
            flushLocalBuffer();
            out.write(b, off, BUFFER_SIZE);
            off += BUFFER_SIZE;
            len -= BUFFER_SIZE;
        }
        while (len > 0) {
            final int transferredCount = Math.min(BUFFER_SIZE - position, len);
            System.arraycopy(b, off, buf, position, transferredCount);
            off += transferredCount;
            len -= transferredCount;
            position += transferredCount;
            ensureBufHasRoom();
        }
    }

    @Override public void flush() throws IOException {
        flushLocalBuffer();
        super.flush();
    }

    private void ensureBufHasRoom() throws IOException {
        if (position != BUFFER_SIZE) {
            return;
        }
        out.write(buf);
        position = 0;
    }

    private void flushLocalBuffer() throws IOException {
        if (position > 0) {
            out.write(buf, 0, position);
            position = 0;
        }
    }
}
