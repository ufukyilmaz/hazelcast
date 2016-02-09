package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.PrefixTombstoneManager;
import com.hazelcast.spi.hotrestart.impl.gc.record.GcRecord;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOnHeap;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.bufferedOutputStream;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A growing chunk which buffers all entries until closed. The backing file
 * is created in {@link #flushAndClose(MutatorCatchup, GcLogger)}).
 */
public final class GrowingDestChunk extends GrowingChunk {
    private final int stuckDetectionThreshold =
            Integer.getInteger("com.hazelcast.spi.hotrestart.gc.stuckDetectThreshold", 1000 * 1000);
    private final GcHelper gch;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private List<GcRecord> sortedGcRecords = new ArrayList<GcRecord>();

    public GrowingDestChunk(long seq, GcHelper gch, PrefixTombstoneManager pfixTombstoMgr) {
        super(seq, new RecordMapOnHeap());
        this.gch = gch;
        this.pfixTombstoMgr = pfixTombstoMgr;
    }

    /**
     * Only called from Evacuator. It adds records in sort order.
     */
    public boolean add(GcRecord gcr) {
        final boolean ret = addStep1(gcr.size());
        ((RecordMapOnHeap) records).put(gcr.toKeyHandle(), gcr);
        sortedGcRecords.add(gcr);
        liveRecordCount++;
        return ret;
    }

    @Override public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int size, int ignored) {
        insertOrUpdateValue(prefix, kh, seq, size);
    }

    /**
     * Creates the destination chunk file. Does its best not to
     * write any records which are known to be garbage at the point when
     * they are ready to be written to the file.
     */
    public StableValChunk flushAndClose(MutatorCatchup mc, GcLogger logger) {
        DataOutputStream out = null;
        final FileOutputStream fileOut = gch.createFileOutputStream(base(), seq, DEST_FNAME_SUFFIX);
        try {
            final long start = System.nanoTime();
            out = dataOutputStream(fileOut);
            mc.catchupNow();
            // Caught up with mutator. Now we collect garbage by taking only live records
            final List<GcRecord> recs = sortedLiveRecords();
            // ... and dismiss the garbage we just collected. It is essential that no catching up
            // occurs within these two steps; otherwise what we dismiss as garbage will not be
            // equal to what we collected.
            mc.dismissGarbage(this);
            // And now we may catch up again.
            mc.catchupNow();
            long fileSize = 0;
            final RecordDataHolder holder = gch.recordDataHolder;
            for (GcRecord r : recs) {
                final KeyHandle kh = r.toKeyHandle();
                final long prefix = r.keyPrefix(kh);
                if (r.isAlive()) {
                    holder.clear();
                    final RamStore ramStore;
                    if ((ramStore = gch.ramStoreRegistry.ramStoreForPrefix(prefix)) != null
                            && ramStore.copyEntry(kh, r.payloadSize(), holder)) {
                        holder.flip();
                        assert holder.payloadSizeValid(r);
                        // catches up for each bufferful
                        fileSize = r.intoOut(out, fileSize, prefix, holder, mc);
                        continue;
                    } else {
                        // Our record is alive, but in the in-memory store the corresponding entry
                        // was already updated and a retirement event is on its way. We did not copy
                        // the record to the file, so to bring our bookkeeping back in sync we must
                        // keep catching up until we observe the event.
                        if (!catchUpUntilRetired(r, mc)) {
                            final String ramStoreName = ramStore != null ? ramStore.getClass().getSimpleName() : "null";
                            throw new HotRestartException(String.format(
                                "Stuck while waiting for a record to be retired. Chunk #%02x, record #%02x,"
                                + " isTombstone? %b, size %d, RAM store was %s",
                                    seq, r.liveSeq(), r.isTombstone(), r.size(), ramStoreName));
                        }
                    }
                }
                // Invariant at this point: r.isAlive() == false and record was not written to file.
                // r.size() relies on keeping the value of Record#size after the record is retired.
                garbage -= r.size();
                // r.isTombstone() relies on keeping the value of Record#size after the record is retired.
                if (!r.isTombstone() && r.deadSeq() > pfixTombstoMgr.tombstoneSeqForPrefix(prefix)) {
                    mc.dismissGarbageRecord(this, r.toKeyHandle(), r);
                }
            }
            out.flush();
            mc.catchupNow();
            fsync(fileOut);
            mc.catchupNow();
            out.close();
            gch.changeSuffix(base(), seq, DEST_FNAME_SUFFIX, gch.newStableChunkSuffix());
            logger.fine("Wrote chunk #%03x (%,d bytes) in %d ms", seq, fileSize,
                    NANOSECONDS.toMillis(System.nanoTime() - start));
            mc.catchupNow();
            // At this point garbage counts are not zero even though we were
            // dismissing each record which was garbage when we encountered it.
            // Since we were catching up with mutator all the time, some records
            // became garbage after we wrote them.
            // Record ownership is still with the current chunk.
            // After this method returns, Evacuator must update the chunks map to
            // redirect ownership to the new stableChunk. It is vital that no
            // catching up occurs before transfer of ownership (otherwise the
            // wrong chunk's garbage field will be updated).
            return new StableValChunk(seq, gch.toPlainRecordMap(records), liveRecordCount, fileSize,
                    garbage, needsDismissing(), gch.compressionEnabled());
        } catch (IOException e) {
            throw new HotRestartException(e);
        } finally {
            closeIgnoringFailure(out);
            closeIgnoringFailure(fileOut);
        }
    }

    private List<GcRecord> sortedLiveRecords() {
        final List<GcRecord> recs = new ArrayList<GcRecord>(liveRecordCount);
        for (GcRecord gcr : sortedGcRecords) {
            if (gcr.isAlive()) {
                recs.add(gcr);
            }
        }
        sortedGcRecords = null;
        return recs;
    }

    @SuppressWarnings("checkstyle:emptyblock")
    private boolean catchUpUntilRetired(GcRecord r, MutatorCatchup mc) {
        for (int eventCount = 0;
             eventCount <= stuckDetectionThreshold && r.isAlive();
             eventCount += catchUpSafely(mc, r)) {
        }
        return !r.isAlive();
    }

    private int catchUpSafely(MutatorCatchup mc, GcRecord r) {
        int eventCount = mc.catchupNow();
        pfixTombstoMgr.dismissGarbage(this);
        if (mc.shutdownRequested()) {
            eventCount += mc.catchupNow();
            pfixTombstoMgr.dismissGarbage(this);
            if (r.isAlive()) {
                throw new HotRestartException(
                        "Record not available, retirement event not received, shutdown requested");
            }
        }
        return eventCount;
    }

    private DataOutputStream dataOutputStream(FileOutputStream fileOut) {
        return new DataOutputStream(gch.compressionEnabled()
                ? gch.compressor.compressedOutputStream(fileOut)
                : bufferedOutputStream(fileOut));
    }
}
