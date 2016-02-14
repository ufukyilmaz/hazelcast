package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileOut;

/**
 * A growing chunk which immediately writes added entries to the backing file.
 * <p>
 * Not thread-safe.
 */
public abstract class WriteThroughChunk extends GrowingChunk {
    final ChunkFileOut out;
    final GcHelper gcHelper;
    private final String suffix;
    private boolean needsFsyncBeforeClosing;

    WriteThroughChunk(long seq, String suffix, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, records);
        this.suffix = suffix;
        this.gcHelper = gcHelper;
        this.out = out;
    }

    public void flagForFsyncOnClose(boolean fsyncOnClose) {
        this.needsFsyncBeforeClosing |= fsyncOnClose;
    }

    public void close() {
        if (out == null) {
            return;
        }
        if (needsFsyncBeforeClosing) {
            fsync();
        }
        out.close();
        gcHelper.changeSuffix(base(), seq, FNAME_SUFFIX + suffix, FNAME_SUFFIX);
    }

    public void fsync() {
        out.fsync();
        needsFsyncBeforeClosing = false;
    }

    final boolean hasRoom() {
        assert !full() : String.format("Attempted to write to a full %s file #%x", base(), seq);
        return true;
    }

    public abstract StableChunk toStableChunk();
}
