package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.internal.hotrestart.impl.io.TombFileAccessor;

import java.io.IOException;

/**
 * Write-through chunk specialized to contain tombstone records.
 */
// class non-final for mockability
public class WriteThroughTombChunk extends WriteThroughChunk implements ActiveChunk {

    public WriteThroughTombChunk(long seq, String suffix, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, suffix, records, out, gcHelper);
    }

    @Override
    public boolean addStep1(long recordSeq, long keyPrefix, byte[] keyBytes, byte[] ignored) {
        assert hasRoom();
        out.writeTombstone(recordSeq, keyPrefix, keyBytes);
        grow(Record.TOMB_HEADER_SIZE + keyBytes.length);
        return full();
    }

    /**
     * Does the same work as {@link #addStep1(long, long, byte[], byte[])}, but using a
     * {@link TombFileAccessor} to get the data.
     * @param tfa tombstone file accessor connected to the source tombstone chunk
     * @param filePos position of the tombstone in the source chunk
     * @return whether this chunk is now full
     */
    public boolean addStep1(TombFileAccessor tfa, int filePos) {
        assert hasRoom();
        try {
            grow(tfa.loadAndCopyTombstone(filePos, out));
            return full();
        } catch (IOException e) {
            throw new HotRestartException("Failed to copy tombstone", e);
        }
    }

    @Override
    public void insertOrUpdate(long recordSeq, long keyPrefix, KeyHandle kh, int filePos, int size) {
        insertOrUpdateTombstone(recordSeq, keyPrefix, kh, filePos, size);
    }

    @Override
    protected int determineSizeLimit() {
        return tombChunkSizeLimit();
    }

    @Override
    public void needsDismissing(boolean needsDismissing) {
    }

    @Override
    public String base() {
        return TOMB_BASEDIR;
    }

    @Override
    public StableTombChunk toStableChunk() {
        return new StableTombChunk(this);
    }
}
