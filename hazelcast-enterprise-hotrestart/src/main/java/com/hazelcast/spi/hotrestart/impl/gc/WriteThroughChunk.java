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

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.ioDisabled;

/**
 * A growing chunk which immediately writes added entries to the backing file.
 * <p>
 * Not thread-safe.
 */
public class WriteThroughChunk extends GrowingChunk {
    private static final ByteBuffer HEADER_BUF = ByteBuffer.allocate(Record.HEADER_SIZE);
    private FileChannel out;
    private final HashMap<KeyHandle, Long> garbageKeyCounts = new HashMap<KeyHandle, Long>();
    private long youngestSeq;

    WriteThroughChunk(long seq, FileChannel out) {
        super(seq);
        this.out = out;
    }

    /**
     * Writes the record to the backing file, then calls
     * {@link GrowingChunk#addStep1(Record)}.
     * Called only by the mutator thread.
     *
     * @param r {@inheritDoc}
     * @return {@inheritDoc}
     * @throws HotRestartException {@inheritDoc}
     */
    public final boolean add(Record r, byte[] keyBytes, byte[] valueBytes) {
        final boolean ret = addStep1(r);
        youngestSeq = r.seq;
        flush(r.toByteBuffers(HEADER_BUF, keyBytes, valueBytes));
        return ret;
    }

    public final void close() {
        if (out == null) {
            return;
        }
        try {
            out.close();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    public final void fsync() {
        fsync(out);
    }

    private void flush(ByteBuffer[] batch) {
        if (ioDisabled()) {
            return;
        }
        try {
            do {
                out.write(batch);
            } while (hasRemaining(batch));
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private static boolean hasRemaining(ByteBuffer[] batch) {
        for (ByteBuffer buf : batch) {
            if (buf.hasRemaining()) {
                return true;
            }
        }
        return false;
    }

    final StableChunk toStableChunk() {
        return new StableChunk(seq, records, youngestSeq, size(), garbage, garbageKeyCounts, false);
    }
}
