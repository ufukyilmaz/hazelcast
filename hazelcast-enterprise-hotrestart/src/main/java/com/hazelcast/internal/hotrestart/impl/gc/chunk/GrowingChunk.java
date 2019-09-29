package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;

/**
 * Represents a chunk which is still growing, either because the underlying file is just being written
 * to or because an existing file is being reloaded during restart.
 */
public abstract class GrowingChunk extends Chunk {
    private final int sizeLimit = determineSizeLimit();
    /**  The current size of the chunk in bytes. */
    private long size;

    // Current chunk file offset from the viewpoint of the addStep2() method. Nedeed to
    // record a tombstone's file offset when it's added to the chunk.
    private int addStep2FilePos;

    protected GrowingChunk(long chunkSeq, RecordMap records) {
        super(chunkSeq, records);
    }

    /** Adds the record to this chunk's record map. Called by the collector thread. */
    public final void addStep2(long recordSeq, long keyPrefix, KeyHandle kh, int size) {
        insertOrUpdate(recordSeq, keyPrefix, kh, addStep2FilePos, size);
        addStep2FilePos += size;
        liveRecordCount++;
    }

    /**
     * Called only by {@code Rebuilder}, which encounters records out of file order. Therefore
     * {@code addStep2FilePos} is unusable.
     */
    public final void addStep2(long recordSeq, long keyPrefix, KeyHandle kh, int filePos, int size) {
        insertOrUpdate(recordSeq, keyPrefix, kh, filePos, size);
        liveRecordCount++;
    }

    /**
     * Inserts a new entry in the chunk's record map or updates the existing one under the same
     * key handle.
     */
    public abstract void insertOrUpdate(long recordSeq, long keyPrefix, KeyHandle kh, int filePos, int size);

    @Override
    public final long size() {
        return size;
    }

    /** Updates the chunk's {@code size} property by adding the supplied amount. */
    protected final void grow(int amount) {
        size += amount;
    }

    /** @return {@code true} if this chunk's size has reached (or exceeded) its size limit. */
    public final boolean full() {
        return size() >= sizeLimit;
    }

    /** Implements {@code insertOrUpdate} for a value record. */
    protected final void insertOrUpdateValue(long recordSeq, long keyPrefix, KeyHandle kh, int size) {
        final Record existing = records.putIfAbsent(keyPrefix, kh, recordSeq, size, false, 0);
        if (existing != null) {
            existing.update(recordSeq, size);
        }
    }

    /** Implements {@code insertOrUpdate} for a tombstone record. */
    protected final void insertOrUpdateTombstone(long recordSeq, long keyPrefix, KeyHandle kh, int filePos, int size) {
        final Record existing = records.putIfAbsent(keyPrefix, kh, recordSeq, size, true, filePos);
        if (existing != null) {
            existing.update(recordSeq, size);
            existing.setFilePosition(filePos);
        }
    }

    /**
     * Determines the limit on the size of this chunk by reading a system property. The size limit
     * is used to decide when to deactivate a chunk and create a new one.
     */
    protected abstract int determineSizeLimit();
}
