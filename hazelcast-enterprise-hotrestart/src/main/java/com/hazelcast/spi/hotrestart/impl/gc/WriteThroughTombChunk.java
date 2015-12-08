package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Write-through chunk specialized to contain tombstone records.
 */
final class WriteThroughTombChunk extends WriteThroughChunk {
    int tombstoneCount;

    WriteThroughTombChunk(long seq, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, records, out, gcHelper);
    }

    @Override public boolean addStep1(long keyPrefix, long recordSeq, byte[] keyBytes, byte[] ignored) {
        ensureHasRoom();
        try {
            dataOut.writeLong(recordSeq);
            dataOut.writeLong(keyPrefix);
            dataOut.writeInt(keyBytes.length);
            dataOut.write(keyBytes);
            size += Record.TOMB_HEADER_SIZE + keyBytes.length;
            tombstoneCount++;
            return full();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @Override boolean full() {
        return tombstoneCount >= TOMB_COUNT_LIMIT;
    }

    @Override String base() {
        return TOMB_BASEDIR;
    }

    @Override StableTombChunk toStableChunk() {
        return new StableTombChunk(this, false);
    }
}
