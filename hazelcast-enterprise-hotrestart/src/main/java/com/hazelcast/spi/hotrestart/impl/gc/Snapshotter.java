package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.internal.util.collection.LongHashSet;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Takes a snapshot of the Hot Restart Store state and saves it to the file named {@value #CHUNK_SNAPSHOT_FNAME}
 * in the Hot Restart Store's home directory. The file has a header followed by records describing each existing
 * chunk file. Records are arbitrarily ordered. JDK's standard {@link java.io.DataOutputStream} is used to
 * write the file.
 * <p>
 * This is the structure of the header:
 * <ol><li>
 *     snapshot's timestamp from {@code System.nanoTime()} &mdash; a {@code long}
 * </li><li>
 *     number of records in the snapshot &mdash; an {@code int}.
 * </li></ol>
 * This is the structure of a record describing a chunk file:
 * <ol><li>
 *     seq &mdash; a {@code long}
 * </li><li>
 *     size / 256 &mdash; a {@code char}
 * </li><li>
 *     garbage / 256 &mdash; a {@code char}
 * </li><li>
 *     flags bitfield &mdash; a {@code byte}:
 *     <ul><li>
 *         bit 0: whether it's a source chunk of the current GC cycle
 *     </li><li>
 *         bit 1: whether it's a survivor chunk of the current GC cycle
 *     </li><li>
 *         bit 2: whether it's a tombstone chunk
 *     </li></ul>
 * </li></ol>
 */
// class non-final for mockability
@SuppressWarnings("checkstyle:finalclass")
public class Snapshotter {
    public static final String PROPERTY_GCSTATS_ENABLED = "hazelcast.hotrestart.gc.stats.enabled";
    public static final HazelcastProperty GCSTATS_ENABLED = new HazelcastProperty(PROPERTY_GCSTATS_ENABLED, false);
    public static final String CHUNK_SNAPSHOT_FNAME = "chunk-snapshot.bin";
    public static final int SOURCE_CHUNK_FLAG_MASK = 1;
    public static final int SURVIVOR_FLAG_MASK = 1 << 1;
    public static final int TOMBSTONE_FLAG_MASK = 1 << 2;

    private static final String NEW_SNAPSHOT_PREFIX = ".new";
    private static final int SNAPSHOTTING_INTERVAL_MILLIS = 50;
    private static final LongHashSet EMPTY_SET = new LongHashSet(0, -1);

    final boolean enabled;
    private final File homeDir;
    private final ChunkManager chunkMgr;

    private long lastChunkSnapshot;
    private LongHashSet srcChunkSeqs = EMPTY_SET;

    @Inject
    private Snapshotter(@Name("homeDir") File homeDir, ChunkManager chunkMgr, HazelcastProperties properties) {
        this.homeDir = homeDir;
        this.chunkMgr = chunkMgr;
        this.enabled = properties.getBoolean(GCSTATS_ENABLED);
    }

    void initSrcChunkSeqs(Collection<? extends Chunk> srcChunks) {
        if (!enabled) {
            return;
        }
        final LongHashSet srcChunkSeqs = new LongHashSet(srcChunks.size(), -1);
        for (Chunk chunk : srcChunks) {
            srcChunkSeqs.add(chunk.seq);
        }
        this.srcChunkSeqs = srcChunkSeqs;
    }

    void resetSrcChunkSeqs() {
        this.srcChunkSeqs = EMPTY_SET;
    }

    void takeChunkSnapshotAsNeeded() {
        final long now = System.nanoTime();
        if (chunkMgr.activeValChunk == null
                || now - lastChunkSnapshot <= MILLISECONDS.toNanos(SNAPSHOTTING_INTERVAL_MILLIS)
        ) {
            return;
        }
        lastChunkSnapshot = now;
        final Map<Long, WriteThroughChunk> survivors = chunkMgr.survivors != null
                ? chunkMgr.survivors : Collections.<Long, WriteThroughChunk>emptyMap();
        final File newFile = new File(homeDir, CHUNK_SNAPSHOT_FNAME + NEW_SNAPSHOT_PREFIX);
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
            out.writeLong(now);
            out.writeInt(chunkMgr.chunks.size() + survivors.size() + 2);
            for (Chunk chunk : chunkMgr.chunks.values()) {
                writeChunkStats(out, chunk, false);
            }
            for (Chunk chunk : survivors.values()) {
                writeChunkStats(out, chunk, true);
            }
            writeChunkStats(out, chunkMgr.activeValChunk, false);
            writeChunkStats(out, chunkMgr.activeTombChunk, false);
            out.close();
            IOUtil.rename(newFile, new File(homeDir, CHUNK_SNAPSHOT_FNAME));
        } catch (IOException e) {
            throw new HotRestartException(e);
        } finally {
            IOUtil.closeResource(out);
        }
    }

    private void writeChunkStats(DataOutputStream out, Chunk chunk, boolean isSurvivor) throws IOException {
        out.writeLong(chunk.seq);
        out.writeChar(encodeSize(chunk.size()));
        out.writeChar(encodeSize(chunk.garbage));
        final boolean isSrcChunk = srcChunkSeqs.contains(chunk.seq);
        final boolean isTombstoneChunk = chunk instanceof StableTombChunk || chunk instanceof WriteThroughTombChunk;
        out.writeByte((isSrcChunk ? SOURCE_CHUNK_FLAG_MASK    : 0)
                    | (isSurvivor ? SURVIVOR_FLAG_MASK        : 0)
                    | (isTombstoneChunk ? TOMBSTONE_FLAG_MASK : 0)
        );
    }

    private static char encodeSize(long size) {
        final long sizeInPages = size >> Byte.SIZE;
        return (char) (sizeInPages <= Character.MAX_VALUE ? sizeInPages : Character.MAX_VALUE);
    }
}
