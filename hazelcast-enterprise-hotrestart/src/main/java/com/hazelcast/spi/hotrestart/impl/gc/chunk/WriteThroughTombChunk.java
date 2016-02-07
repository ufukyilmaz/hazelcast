package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Write-through chunk specialized to contain tombstone records.
 */
public final class WriteThroughTombChunk extends WriteThroughChunk {
    long catchupPosition;

    public WriteThroughTombChunk(long seq, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, records, out, gcHelper);
    }

    @Override public boolean addStep1(long keyPrefix, long recordSeq, byte[] keyBytes, byte[] ignored) {
        ensureHasRoom();
        try {
            writeTombstone(dataOut, recordSeq, keyPrefix, keyBytes);
            size += Record.TOMB_HEADER_SIZE + keyBytes.length;
            return full();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @Override public void addStep2(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone) {
        final Record existing = records.putIfAbsent(prefix, kh, seq, size, isTombstone, 0);
        if (existing != null) {
            existing.update(seq, size, isTombstone);
        }
        liveRecordCount++;
    }

    public static void writeTombstone(DataOutputStream out, long recordSeq, long keyPrefix, byte[] keyBytes)
            throws IOException {
        out.writeLong(recordSeq);
        out.writeLong(keyPrefix);
        out.writeInt(keyBytes.length);
        out.write(keyBytes);
    }

    @Override public boolean full() {
        return size() >= TOMB_SIZE_LIMIT;
    }

    @Override public String base() {
        return TOMB_BASEDIR;
    }

    @Override public StableTombChunk toStableChunk() {
        return new StableTombChunk(this, false);
    }
}
