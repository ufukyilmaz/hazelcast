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
import com.hazelcast.spi.hotrestart.InMemoryStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import com.hazelcast.util.collection.Long2ObjectHashMap.KeyIterator;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.compression;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.newStableChunkSuffix;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A growing chunk which buffers all entries until closed. The backing file
 * is created in {@link #flushAndClose(InMemoryStoreRegistry, MutatorCatchup)}).
 */
public class GrowingDestChunk extends GrowingChunk {
    /** Suffix added to a chunk file while it is being written to during a GC cycle.
     * If system fails during GC, such file should not be considered during restart. */
    public static final String DEST_FNAME_SUFFIX = Chunk.FNAME_SUFFIX + ".dest";
    private final GcHelper fac;
    private Long2ObjectHashMap<Record> retiredRecords;

    GrowingDestChunk(long seq, GcHelper fac) {
        super(seq);
        this.fac = fac;
    }

    /**
     * Puts the record into the records map, calls {@link #addStep1(Record)}.
     *
     * @param r {@inheritDoc}
     * @return {@inheritDoc}
     * @throws HotRestartException {@inheritDoc}
     */
    final boolean add(Record r) {
        final boolean ret = addStep1(r);
        records.put(r.seq, r);
        return ret;
    }

    @Override void retire(Record r) {
        super.retire(r);
        if (retiredRecords != null) {
            retiredRecords.put(r.seq, r);
        }
    }

    /**
     * Creates the destination chunk file. Does its best not to
     * write any records which are known to be garbage at the point when
     * they are ready to be written to the file.
     */
    public final StableChunk flushAndClose(InMemoryStoreRegistry reg, MutatorCatchup mc) {
        final FileOutputStream fileOut = fac.createFileOutputStream(seq, DEST_FNAME_SUFFIX);
        final long start = System.nanoTime();
        final DataOutputStream out = new DataOutputStream(
                compression ? compressedOutputStream(fileOut) : bufferedOutputStream(fileOut));
        mc.catchupNow();
        // Caught up with mutator. Now we collect garbage by taking only live records
        final long[] seqs = sortedLiveSeqs();
        // ... and dismiss the garbage we just collected. It is essential that no catching up
        // occurs within these two steps; otherwise what we dismiss as garbage will not be
        // equal to what we collected.
        mc.dismissGarbage(this);
        // Now we must start tracking individual garbage records (not just counts)
        // so we can individually dismiss them in the loop below.
        retiredRecords = new Long2ObjectHashMap<Record>();
        // And now we may catch up again.
        mc.catchupNow();
        long fileSize = 0;
        long youngestRecordSeq = -1;
        for (long seq : seqs) {
            final Record r = records.get(seq);
            if (r != null) {
                if (reg.inMemoryStoreForPrefix(r.keyHandle.keyPrefix())
                       .copyEntry(r.keyHandle, fac.recordDataHolder)) {
                    youngestRecordSeq = r.seq;
                    // catches up for each bufferful
                    fileSize = r.intoOut(out, fileSize, fac.recordDataHolder, mc);
                    continue;
                } else {
                    // even though the record was there in our map, the corresponding
                    // entry disappeared from the in-memory store. We haven't yet
                    // observed that event in our work queue, so we must keep catching
                    // up until we do.
                    do {
                        mc.catchupNow();
                    } while (records.containsKey(seq));
                }
            }
            // We just collected another garbage record. It was live at the time
            // we called sortedLiveSeqs(), but we're not writing it to the file.
            mc.dismissGarbageRecord(this, retiredRecords.get(seq));
        }
        // Done selecting records to write out, no more need to track
        // individual retirement.
        retiredRecords = null;
        try {
            out.flush();
            mc.catchupNow();
            // At this point garbage counts are not zero even though we were
            // dismissing each record which was garbage when we encountered it.
            // Since we were catching up with mutator all the time, some records
            // became garbage after we have written them.
            // With this call we transfer record ownership to the stable chunk.
            // Further retirements will be addressed at that chunk.
            final StableChunk stableChunk =
                    new StableChunk(seq, records, youngestRecordSeq, fileSize, garbage, garbageKeyCounts, compression);
            fsync(fileOut.getChannel());
            out.close();
            fac.changeSuffix(seq, DEST_FNAME_SUFFIX, newStableChunkSuffix());
            System.err.format("Wrote chunk #%d (%,d bytes) in %d ms%n",
                    seq, fileSize, NANOSECONDS.toMillis(System.nanoTime() - start));
            mc.catchupNow();
            return stableChunk;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private long[] sortedLiveSeqs() {
        long[] seqs = new long[records.size()];
        int insertionPoint = 0;
        for (KeyIterator iterator = records.keySet().iterator(); iterator.hasNext();) {
            seqs[insertionPoint++] = iterator.nextLong();
        }
        Arrays.sort(seqs);
        return seqs;
    }

    private static OutputStream gzipOutputStream(FileOutputStream out, MutatorCatchup mc) {
        try {
            return out == null ? nullOutputStream() : new BufferedGzipOutputStream(out, mc);
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private static OutputStream compressedOutputStream(FileOutputStream out) {
        return out == null ? nullOutputStream() : Compression.compressedOutputStream(out);
    }

    private static OutputStream bufferedOutputStream(FileOutputStream out) {
        return out == null ? nullOutputStream() : new BufferedOutputStream(out);
    }

    private static OutputStream nullOutputStream() {
        return new OutputStream() {
            @Override public void write(int i) throws IOException { }
        };
    }
}
