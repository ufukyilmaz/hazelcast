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

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Represents a chunk file which is still growing.
 */
abstract class GrowingChunk extends Chunk {
    long size;

    GrowingChunk(long seq) {
        super(seq);
    }

    /**
     * Updates this chunk's size by adding the supplied record's size.
     * Assigns this chunk as the owner of the supplied record.
     * For a WriteThroughChunk this is called by the mutator thread;
     * otherwise called by the collector thread.
     *
     * @param r the record to add
     * @return true if this chunk has now reached capacity
     * @throws HotRestartException {@inheritDoc}
     */
    final boolean addStep1(Record r) {
        if (full()) {
            throw new HotRestartException("Attempted to write to a full file (no. " + seq + ')');
        }
        r.chunk = this;
        size += r.size();
        return full();
    }

    @Override public final long size() {
        return size;
    }

    final boolean full() {
        return size >= SIZE_LIMIT;
    }

    public static void fsync(FileChannel out) {
        if (out == null) {
            return;
        }
        try {
            out.force(true);
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }
}
