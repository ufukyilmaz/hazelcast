package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;

/**
 * Represents a chunk associated with an open file. Synchronously writes each record to the
 * underlying file.
 */
public abstract class WriteThroughChunk extends GrowingChunk {
    public final ChunkFileOut out;
    final GcHelper gcHelper;
    private final String suffix;

    WriteThroughChunk(long seq, String suffix, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, records);
        this.suffix = suffix;
        this.gcHelper = gcHelper;
        this.out = out;
    }

    public void flagForFsyncOnClose(boolean fsyncOnClose) {
        out.flagForFsyncOnClose(fsyncOnClose);
    }

    public void close() {
        out.close();
        gcHelper.changeSuffix(base(), seq, FNAME_SUFFIX + suffix, FNAME_SUFFIX);
    }

    public void fsync() {
        out.fsync();
    }

    // Called exclusively as "assert hasRoom()".
    final boolean hasRoom() {
        assert !full() : String.format("Attempted to write to a full %s file #%x", base(), seq);
        return true;
    }

    public abstract StableChunk toStableChunk();
}
