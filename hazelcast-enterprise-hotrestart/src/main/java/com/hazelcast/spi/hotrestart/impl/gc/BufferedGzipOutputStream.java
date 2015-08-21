/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
class BufferedGzipOutputStream extends GZIPOutputStream {
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
