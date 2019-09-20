package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap;
import com.hazelcast.spi.hotrestart.impl.io.TombFileAccessor;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;

import java.util.Collection;

import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.positionInUnitsOfBufsize;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Performs the TombGC procedure. Selected source chunk files are opened one at a time and all their live
 * tombstones are copied to the survivor chunks.
 */
final class TombEvacuator {
    private final Collection<StableTombChunk> srcChunks;

    @Inject private GcLogger logger;
    @Inject private GcHelper gcHelper;
    @Inject private MutatorCatchup mc;
    @Inject private ChunkManager chunkMgr;

    private Long2ObjectHashMap<WriteThroughChunk> survivorMap;
    private TrackerMap trackers;
    private WriteThroughTombChunk survivor;
    private long start;

    TombEvacuator(Collection<StableTombChunk> srcChunks) {
        this.srcChunks = srcChunks;
    }

    void evacuate() {
        this.survivorMap = chunkMgr.survivors = new Long2ObjectHashMap<WriteThroughChunk>();
        this.trackers = chunkMgr.trackers;
        for (StableTombChunk chunk : srcChunks) {
            evacuate(chunk);
        }
        if (survivor != null) {
            closeSurvivor();
        }
    }

    private void evacuate(StableTombChunk chunk) {
        final TombFileAccessor tfa = new TombFileAccessor(gcHelper.stableChunkFile(chunk, false));
        final int[] filePositions = chunk.initFilePosToKeyHandle();
        try {
            for (int filePos : filePositions) {
                final KeyHandle kh = chunk.getLiveKeyHandle(filePos);
                if (kh == null) {
                    continue;
                }
                ensureSurvivor();
                final long posBefore = positionInUnitsOfBufsize(survivor.size());
                final boolean full = survivor.addStep1(tfa, filePos);
                survivor.addStep2(tfa.recordSeq(), tfa.keyPrefix(), kh, tfa.recordSize());
                trackers.get(kh).moveToChunk(survivor.seq);
                if (positionInUnitsOfBufsize(survivor.size()) != posBefore) {
                    mc.catchupNow();
                }
                if (full) {
                    closeSurvivor();
                }
            }
        } finally {
            tfa.close();
            chunk.disposeFilePosToKeyHandle();
        }
    }

    private void ensureSurvivor() {
        if (survivor != null) {
            return;
        }
        start = System.nanoTime();
        survivor = gcHelper.newWriteThroughTombChunk(Chunk.SURVIVOR_FNAME_SUFFIX);
        survivor.flagForFsyncOnClose(true);
        survivorMap.put(survivor.seq, survivor);
    }

    private void closeSurvivor() {
        survivor.close();
        mc.catchupNow();
        logger.finest("Wrote tombstone chunk #%03x (%,d bytes) in %d ms", survivor.seq, survivor.size(),
                NANOSECONDS.toMillis(System.nanoTime() - start));
        survivor = null;
        mc.catchupNow();
    }
}
