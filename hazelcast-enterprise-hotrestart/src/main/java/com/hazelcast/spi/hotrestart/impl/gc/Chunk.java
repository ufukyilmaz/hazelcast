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
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a chunk file.
 */
public abstract class Chunk {
    /** Chunk filename suffix. */
    public static final String FNAME_SUFFIX = ".chunk";
    /** Chunk file size limit. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long SIZE_LIMIT = 8 << 20;

    /** Unique sequence number of this chunk. */
    public final long seq;

    final Long2ObjectHashMap<Record> records;
    final HashMap<KeyHandle, Long> garbageKeyCounts;
    long garbage;

    Chunk(long seq) {
        this.seq = seq;
        this.records = new Long2ObjectHashMap<Record>();
        this.garbageKeyCounts = new HashMap<KeyHandle, Long>();
    }

    Chunk(long seq, Long2ObjectHashMap<Record> records, long garbage, HashMap<KeyHandle, Long> garbageKeyCounts) {
        this.seq = seq;
        this.garbage = garbage;
        this.records = records;
        this.garbageKeyCounts = garbageKeyCounts;
    }

    abstract long size();

    void retire(Record r) {
        if (records.remove(r.seq) == null) {
            final List<Long> seqs = new ArrayList<Long>(records.keySet());
            Collections.sort(seqs);
            throw new HotRestartException(String.format(
                    "Chunk %d couldn't find the record to retire: %d. Chunk has these (%,d): %s",
                    this.seq, r.seq, records.size(), seqs));
        }
        garbage += r.size();
        incrementGarbageCount(garbageKeyCounts, r);
        r.chunk = null;
    }

    String fnameSuffix() {
        return FNAME_SUFFIX;
    }

    static void incrementGarbageCount(Map<KeyHandle, Long> counts, Record r) {
        if (r.isTombstone()) {
            return;
        }
        final KeyHandle key = r.keyHandle;
        final Long count = counts.get(key);
        counts.put(key, count == null ? 1L : count + 1);
    }

    static boolean decrementGarbageCount(Map<KeyHandle, Long> counts, Record r) {
        if (r.isTombstone()) {
            return false;
        }
        final KeyHandle key = r.keyHandle;
        final Long current = counts.get(key);
        final long newCount = current == null ? 0 : current - 1;
        if (newCount == 0) {
            counts.remove(key);
            return true;
        } else {
            counts.put(key, newCount);
            return false;
        }
    }
}
