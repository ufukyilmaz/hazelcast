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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.incrementGarbageCount;

/**
 * Rebuilds the runtime metadata of the chunk manager when the system
 * is restarting. Each record read from persistent storage must be fed to this
 * class's <code>accept()</code> method. The order of encountering records
 * doesn't matter for correctness, but for efficiency it is preferred that
 * newer records are encountered first.
 */
public class Rebuilder {
    private Map<KeyHandle, Record> records;
    private final ChunkManager cm;
    private RebuildingChunk chunk;
    private long maxSeq;

    public Rebuilder(Map<KeyHandle, Record> records, ChunkManager chunkMgr) {
        this.records = records;
        this.cm = chunkMgr;
    }

    /**
     * Called when another chunk file starts being read.
     * @param seq the sequence id of the chunk file
     * @param gzipped whether the file is compressed
     */
    public void startNewChunk(long seq, boolean gzipped) {
        finishCurrentChunk();
        chunk = new RebuildingChunk(seq, gzipped);
    }

    public long currChunkSeq() {
        return chunk.seq;
    }

    /**
     * Called upon encountering another record in the file.
     * @param incoming the encountered record
     */
    public void accept(Record incoming) {
        cm.occupancy += incoming.size();
        if (incoming.seq > maxSeq) {
            maxSeq = incoming.seq;
        }
        Record current = records.get(incoming.keyHandle);
        if (current == null) {
            current = cm.liveTombstones.get(incoming.keyHandle);
        }
        if (current == null) {
            addFresh(incoming);
        } else if (incoming.seq >= current.seq) {
            if (current.isTombstone()) {
                cm.retireTombstone(current.keyHandle);
            } else {
                cm.retire(current);
            }
            addFresh(incoming);
        } else {
            addStale(incoming);
        }
    }

    /**
     * Called when done reading. Retires any tombstones which
     * are no longer needed.
     */
    public void done() {
        finishCurrentChunk();
        long retiredCount = 0;
        for (Iterator<Entry<KeyHandle, Record>> it = cm.liveTombstones.entrySet().iterator(); it.hasNext();) {
            final Entry<KeyHandle, Record> e = it.next();
            if (!cm.garbageKeyCounts.containsKey(e.getKey())) {
                retiredCount++;
                final Record tombstone = e.getValue();
                tombstone.chunk.retire(tombstone);
                cm.garbage += tombstone.size();
                it.remove();
            }
        }
        System.out.println("Retired " + retiredCount + " tombstones. There are " + cm.liveTombstones.size() + " left");
        cm.chunkFactory.initRecordSeq(maxSeq);
    }

    private void addFresh(Record fresh) {
        chunk.add(fresh);
        if (fresh.isTombstone()) {
            records.remove(fresh.keyHandle);
            cm.liveTombstones.put(fresh.keyHandle, fresh);
        } else {
            records.put(fresh.keyHandle, fresh);
        }
    }

    private void addStale(Record stale) {
        chunk.addStep1(stale);
        chunk.garbage += stale.size();
        cm.garbage += stale.size();
        incrementGarbageCount(chunk.garbageKeyCounts, stale);
        incrementGarbageCount(cm.garbageKeyCounts, stale);
        stale.chunk = null;
    }

    private void finishCurrentChunk() {
        if (chunk != null) {
            final StableChunk stable = chunk.toStableChunk();
            cm.chunks.add(stable);
        }
    }

    private static class RebuildingChunk extends GrowingChunk {
        private long youngestSeq;
        private boolean gzipped;

        RebuildingChunk(long seq, boolean gzipped) {
            super(seq);
            this.gzipped = gzipped;
        }

        final boolean add(Record r) {
            final boolean ret = addStep1(r);
            youngestSeq = r.seq;
            records.put(r.seq, r);
            return ret;
        }

        StableChunk toStableChunk() {
            return new StableChunk(seq, records, youngestSeq, size(), garbage, garbageKeyCounts, gzipped);
        }
    }
}
