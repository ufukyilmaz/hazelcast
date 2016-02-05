package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.ChunkFileCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.File;
import java.util.Collection;

import static com.hazelcast.spi.hotrestart.impl.gc.Evacuator.propagateDismissing;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.positionInUnitsOfBufsize;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

final class TombEvacuator {
    private final Collection<StableTombChunk> srcChunks;
    private final GcLogger logger;
    private final Long2ObjectHashMap<Chunk> destChunkMap;
    private final TrackerMap recordTrackers;
    private final GcHelper gcHelper;
    private final PrefixTombstoneManager pfixTombstoMgr;
    private final MutatorCatchup mc;

    private TombEvacuator(Collection<StableTombChunk> srcChunks, ChunkManager chunkMgr,
                          MutatorCatchup mc, GcLogger logger
    ) {
        this.srcChunks = srcChunks;
        this.logger = logger;
        this.destChunkMap = chunkMgr.destChunkMap = new Long2ObjectHashMap<Chunk>();
        this.gcHelper = chunkMgr.gcHelper;
        this.pfixTombstoMgr = chunkMgr.pfixTombstoMgr;
        this.recordTrackers = chunkMgr.trackers;
        this.mc = mc;
    }

    static void copyLiveTombstones(
            Collection<StableTombChunk> srcChunks, ChunkManager chunkMgr, MutatorCatchup mc, GcLogger logger
    ) {
        new TombEvacuator(srcChunks, chunkMgr, mc, logger).evacuate();
    }

    private void evacuate() {
        for (StableTombChunk chunk : srcChunks) {
            pfixTombstoMgr.dismissGarbage(chunk);
        }
        WriteThroughTombChunk dest = null;
        long start = 0;
        for (StableTombChunk chunk : srcChunks) {
            final File srcFile = gcHelper.chunkFile(chunk, false);
            chunk.initLiveSeqToKeyHandle();
            for (ChunkFileCursor.Tomb fc = new ChunkFileCursor.Tomb(srcFile, gcHelper); fc.advance();) {
                final KeyHandle kh = chunk.getLiveKeyHandle(fc.recordSeq());
                if (kh == null) {
                    continue;
                }
                if (dest == null) {
                    start = System.nanoTime();
                    dest = newDestChunk();
                }
                final long posBefore = positionInUnitsOfBufsize(dest.size());
                final boolean full = dest.addStep1(fc.prefix(), fc.recordSeq(), fc.key(), null);
                dest.addStep2(fc.prefix(), kh, fc.recordSeq(), fc.size(), true);
                recordTrackers.get(kh).moveToChunk(dest.seq);
                if (positionInUnitsOfBufsize(dest.size()) != posBefore) {
                    mc.catchupNow();
                }
                if (full) {
                    closeDest(dest, start);
                    gcHelper.changeSuffix(dest.base(), dest.seq, Chunk.DEST_FNAME_SUFFIX, Chunk.FNAME_SUFFIX);
                    final StableTombChunk stable = dest.toStableChunk();
                    // Transfers record ownership to the stable chunk
                    destChunkMap.put(stable.seq, stable);
                    dest = null;
                    mc.catchupNow();
                }
            }
        }
        if (dest != null) {
            closeDest(dest, start);
        }
        propagateDismissing(srcChunks, destChunkMap.values(), pfixTombstoMgr, mc);
        for (StableTombChunk evacuated : srcChunks) {
            gcHelper.deleteChunkFile(evacuated);
            mc.catchupNow();
        }
    }

    private void closeDest(WriteThroughTombChunk dest, long start) {
        dest.close();
        logger.fine("Wrote tombstone chunk #%03x (%,d bytes) in %d ms", dest.seq, dest.size(),
                NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private WriteThroughTombChunk newDestChunk() {
        final WriteThroughTombChunk dest = gcHelper.newWriteThroughTombChunk(Chunk.DEST_FNAME_SUFFIX);
        dest.flagForFsyncOnClose(true);
        destChunkMap.put(dest.seq, dest);
        return dest;
    }
}
