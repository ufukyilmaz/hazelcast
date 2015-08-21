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

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.HashMap;

import static com.hazelcast.spi.hotrestart.impl.gc.Compression.COMPRESSED_SUFFIX;

/**
 * Represents a chunk whose on-disk contents are stable (immutable).
 */
class StableChunk extends Chunk {
    final long youngestRecordSeq;
    boolean compressed;
    private final long size;
    private double costBenefit;

    StableChunk(long seq, Long2ObjectHashMap<Record> records, long youngestRecordSeq, long size, long garbage,
                HashMap<KeyHandle, Long> garbageKeyCounts, boolean compressed) {
        super(seq, records, garbage, garbageKeyCounts);
        this.size = size;
        this.youngestRecordSeq = youngestRecordSeq;
        this.compressed = compressed;
        for (Record r : records.values()) {
            r.chunk = this;
        }
    }

    @Override final long size() {
        return size;
    }

    final long cost() {
        return size() - garbage;
    }

    final double updateCostBenefit(long currSeq) {
        return costBenefit = costBenefit(currSeq);
    }

    final double costBenefit(long currSeq) {
        final double benefit = this.garbage;
        final double cost = cost();
        if (cost == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double age = currSeq - youngestRecordSeq;
        return age * benefit / cost;
    }

    final double cachedCostBenefit() {
        return costBenefit;
    }

    @Override final String fnameSuffix() {
        return Chunk.FNAME_SUFFIX + (compressed ? COMPRESSED_SUFFIX : "");
    }
}
