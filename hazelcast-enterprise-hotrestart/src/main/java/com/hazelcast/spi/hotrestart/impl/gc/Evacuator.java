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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Evacuates source chunks into destination chunks by moving all live records.
 * Dismisses the garbage thus collected and deletes the evacuated source chunks.
 */
final class Evacuator {
    private static final Comparator<Record> BY_RECORD_SEQ = new Comparator<Record>() {
        @Override public int compare(Record left, Record right) {
            final long leftSeq = left.seq;
            final long rightSeq = right.seq;
            return leftSeq == rightSeq ? 0 : leftSeq < rightSeq ? -1 : 1;
        }
    };
    private final List<StableChunk> srcChunks;
    private final GcHelper chunkFactory;
    private final MutatorCatchup mc;

    private Evacuator(List<StableChunk> srcChunks, GcHelper chunkFactory, MutatorCatchup mc) {
        this.srcChunks = srcChunks;
        this.chunkFactory = chunkFactory;
        this.mc = mc;
    }

    static List<StableChunk> copyLiveRecords(
            List<StableChunk> srcChunks, GcHelper chunkFactory, MutatorCatchup mc)
    {
        return new Evacuator(srcChunks, chunkFactory, mc).collect();
    }

    private List<StableChunk> collect() {
        final SortedSet<Record> liveRecords = sortedLiveRecords();
        final List<GrowingDestChunk> preparedDestChunks = transferToDest(liveRecords);
        final List<StableChunk> destChunks = persistDestChunks(preparedDestChunks);
        dismissEvacuatedFiles();
        deleteEmptyDestFiles(destChunks);
        return destChunks;
    }

    private SortedSet<Record> sortedLiveRecords() {
        final SortedSet<Record> liveRecords = new TreeSet<Record>(BY_RECORD_SEQ);
        for (StableChunk c : srcChunks) {
            final ArrayList<Record> records = new ArrayList<Record>(c.records.values());
            mc.catchupNow();
            for (Record r : records) {
                mc.catchupAsNeeded();
                // Whenever r.chunk == null, a garbage record is collected
                // because we leave it out from the live record set
                if (r.chunk != null) {
                    liveRecords.add(r);
                }
            }
        }
        return liveRecords;
    }

    private List<GrowingDestChunk> transferToDest(SortedSet<Record> liveRecords) {
        final List<GrowingDestChunk> destChunks = new ArrayList<GrowingDestChunk>();
        GrowingDestChunk dest = null;
        for (Record r : liveRecords) {
            if (dest == null) {
                dest = chunkFactory.newDestChunk();
                destChunks.add(dest);
            }
            mc.catchupAsNeeded();
            // Whenever r.chunk == null, a garbage record is collected
            // because we don't transfer it to the dest chunk.
            // With dest.add(r) the record is transferred and if it is retired after that,
            // it will count towards the garbage count of the dest chunk.
            if (r.chunk != null && dest.add(r)) {
                dest = null;
            }
        }
        return destChunks;
    }

    private List<StableChunk> persistDestChunks(List<GrowingDestChunk> destChunks) {
        final List<StableChunk> compactedChunks = new ArrayList<StableChunk>();
        for (GrowingDestChunk destChunk : destChunks) {
            compactedChunks.add(destChunk.flushAndClose(chunkFactory.inMemoryStoreRegistry, mc));
        }
        return compactedChunks;
    }

    private void dismissEvacuatedFiles() {
        for (StableChunk evacuated : srcChunks) {
            chunkFactory.deleteFile(evacuated);
            // All garbage records collected from the source chunk in
            // sortedLiveRecords() and transferToDest() are summarily dismissed by this call:
            mc.dismissGarbage(evacuated);
            mc.catchupNow();
        }
    }

    private void deleteEmptyDestFiles(List<StableChunk> destChunks) {
        for (Iterator<StableChunk> iterator = destChunks.iterator(); iterator.hasNext();) {
            final StableChunk c = iterator.next();
            if (c.size() - c.garbage == 0) {
                if (!c.garbageKeyCounts.isEmpty()) {
                    System.err.println("Empty dest file with non-empty garbage key counts: " + c.garbageKeyCounts);
                    mc.dismissGarbage(c);
                }
                chunkFactory.deleteFile(c);
                iterator.remove();
                mc.catchupNow();
            }
        }
    }
}
