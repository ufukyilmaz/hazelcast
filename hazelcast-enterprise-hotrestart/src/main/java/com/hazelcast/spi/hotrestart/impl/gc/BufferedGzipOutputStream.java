package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static java.lang.Math.min;

/**
 * Specialization of the standard {@link GZIPOutputStream} which adds buffering.
 * Cooperates with the Hot Restart garbage collector, catching up with the
 * mutator thread on each bufferful.
 */
final class BufferedGzipOutputStream extends GZIPOutputStream {
    /** Output buffer size */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int BUFFER_SIZE = 1 << 13;
    /** Headroom in the output buffer. If less than this many bytes is left
     * free in the buffer, flush it. Helps prevent the next deflater step
     * running out of buffer, resulting in a flush-and-retry cycle. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int BUFFER_HEADROOM = 1 << 9;
    /** Maximum chunk of input data to give to the deflater at once. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int DEFLATER_WINDOW = 1 << 18;
    private final MutatorCatchup mc;
    private int bufPosition;

    public BufferedGzipOutputStream(FileOutputStream out, MutatorCatchup mc) throws IOException {
        super(out, BUFFER_SIZE);
        this.mc = mc;
    }

    @Override public void write(byte[] b, int off, int len) throws IOException {
        int position = off;
        int remaining = len;
        do {
            int transferredCount = min(DEFLATER_WINDOW, len);
            def.setInput(b, position, transferredCount);
            while (!def.needsInput()) {
                deflate();
            }
            position += transferredCount;
            remaining -= transferredCount;
        } while (remaining > 0);
        crc.update(b, off, len);
    }

    @Override protected void deflate() throws IOException {
        final int bytesOutput = def.deflate(buf, bufPosition, buf.length - bufPosition);
        mc.catchupNow();
        bufPosition += bytesOutput;
        if (bufPosition >= BUFFER_SIZE - BUFFER_HEADROOM || bytesOutput == 0 && !def.needsInput()) {
            flush();
        }
    }

    @Override public void flush() throws IOException {
        if (bufPosition > 0) {
            out.write(buf, 0, bufPosition);
            bufPosition = 0;
        }
        super.flush();
        mc.catchupNow();
        if (mc.fsyncOften) {
            ((FileOutputStream) out).getChannel().force(true);
        }
    }
}
