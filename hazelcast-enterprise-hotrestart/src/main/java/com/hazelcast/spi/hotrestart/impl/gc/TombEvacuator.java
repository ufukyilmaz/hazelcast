package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.spi.hotrestart.impl.io.TombFileAccessor;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Collection;

import static com.hazelcast.spi.hotrestart.impl.gc.Evacuator.propagateDismissing;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.positionInUnitsOfBufsize;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

final class TombEvacuator {
    private final Collection<StableTombChunk> srcChunks;
    private final GcLogger logger;
    private final Long2ObjectHashMap<Chunk> destChunkMap;
    private final TrackerMap recordTrackers;
    private final GcHelper gcHelper;
    private final MutatorCatchup mc;
    private WriteThroughTombChunk dest;
    private long start;

    private TombEvacuator(Collection<StableTombChunk> srcChunks, ChunkManager chunkMgr,
                          MutatorCatchup mc, GcLogger logger
    ) {
        this.srcChunks = srcChunks;
        this.logger = logger;
        this.destChunkMap = chunkMgr.destChunkMap = new Long2ObjectHashMap<Chunk>();
        this.gcHelper = chunkMgr.gcHelper;
        this.recordTrackers = chunkMgr.trackers;
        this.mc = mc;
    }

    static void evacuate(
            Collection<StableTombChunk> srcChunks, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger
    ) {
        new TombEvacuator(srcChunks, chunkMgr, mc, logger).evacuateSrcChunks();
    }

    private void evacuateSrcChunks() {
        for (StableTombChunk chunk : srcChunks) {
            evacuate(chunk);
        }
        if (dest != null) {
            closeDestChunk();
        }
        for (StableTombChunk evacuated : srcChunks) {
            gcHelper.deleteChunkFile(evacuated);
            mc.catchupNow();
        }
    }

    private void evacuate(StableTombChunk chunk) {
        final TombFileAccessor tfa = new TombFileAccessor(gcHelper.chunkFile(chunk, false));
        final int[] filePositions = chunk.initFilePosToKeyHandle();
        try {
            for (int filePos : filePositions) {
                final KeyHandle kh = chunk.getLiveKeyHandle(filePos);
                if (kh == null) {
                    continue;
                }
                ensureDestChunk();
                final long posBefore = positionInUnitsOfBufsize(dest.size());
                final boolean full = dest.addStep1(tfa, filePos);
                dest.addStep2(tfa.keyPrefix(), kh, tfa.recordSeq(), tfa.recordSize());
                recordTrackers.get(kh).moveToChunk(dest.seq);
                if (positionInUnitsOfBufsize(dest.size()) != posBefore) {
                    mc.catchupNow();
                }
                if (full) {
                    closeDestChunk();
                }
            }
        } finally {
            tfa.close();
            chunk.disposeFilePosToKeyHandle();
        }
    }

    private void ensureDestChunk() {
        if (dest != null) {
            return;
        }
        start = System.nanoTime();
        dest = gcHelper.newWriteThroughTombChunk(Chunk.DEST_FNAME_SUFFIX);
        dest.flagForFsyncOnClose(true);
        destChunkMap.put(dest.seq, dest);
    }

    private void closeDestChunk() {
        dest.close();
        mc.catchupNow();
        logger.fine("Wrote tombstone chunk #%03x (%,d bytes) in %d ms", dest.seq, dest.size(),
                NANOSECONDS.toMillis(System.nanoTime() - start));
        final StableTombChunk stable = dest.toStableChunk();
        // Transfers record ownership to the stable chunk
        destChunkMap.put(stable.seq, stable);
        dest = null;
        mc.catchupNow();
    }
}
