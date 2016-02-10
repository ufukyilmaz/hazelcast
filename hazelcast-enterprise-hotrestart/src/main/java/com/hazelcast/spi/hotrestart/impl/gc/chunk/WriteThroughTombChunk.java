package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.io.TombFileAccessor;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Write-through chunk specialized to contain tombstone records.
 */
public final class WriteThroughTombChunk extends WriteThroughChunk implements ActiveChunk {

    public WriteThroughTombChunk(long seq, String suffix, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, suffix, records, out, gcHelper);
    }

    @Override public boolean addStep1(long keyPrefix, long recordSeq, byte[] keyBytes, byte[] ignored) {
        assert hasRoom();
        try {
            writeTombstone(dataOut, recordSeq, keyPrefix, keyBytes);
            size += Record.TOMB_HEADER_SIZE + keyBytes.length;
            return full();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    public boolean addStep1(TombFileAccessor tfa, int filePos) {
        assert hasRoom();
        try {
            size += tfa.loadAndCopyTombstone(filePos, dataOut);
            return full();
        } catch (IOException e) {
            throw new HotRestartException("Failed to copy tombstone", e);
        }
    }

    public static void writeTombstone(DataOutputStream out, long recordSeq, long keyPrefix, byte[] keyBytes)
    throws IOException {
        out.writeLong(recordSeq);
        out.writeLong(keyPrefix);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
    }

    @Override public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int size, int fileOffset) {
        insertOrUpdateTombstone(prefix, kh, seq, size, fileOffset);
    }

    @Override protected int determineSizeLimit() {
        return tombChunkSizeLimit();
    }

    @Override public void needsDismissing(boolean needsDismissing) {
    }

    @Override public String base() {
        return TOMB_BASEDIR;
    }

    @Override public StableTombChunk toStableChunk() {
        return new StableTombChunk(this, false);
    }
}
