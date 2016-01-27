package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Write-through chunk specialized to contain value records.
 */
final class WriteThroughValChunk extends WriteThroughChunk {

    WriteThroughValChunk(long seq, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, records, out, gcHelper);
    }

    @Override public boolean addStep1(long keyPrefix, long recordSeq, byte[] keyBytes, byte[] valueBytes) {
        ensureHasRoom();
        try {
            dataOut.writeLong(recordSeq);
            dataOut.writeLong(keyPrefix);
            dataOut.writeInt(keyBytes.length);
            dataOut.writeInt(valueBytes.length);
            dataOut.write(keyBytes);
            dataOut.write(valueBytes);
            size += Record.VAL_HEADER_SIZE + keyBytes.length + valueBytes.length;
            return full();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @Override StableValChunk toStableChunk() {
        return new StableValChunk(this, false);
    }
}
