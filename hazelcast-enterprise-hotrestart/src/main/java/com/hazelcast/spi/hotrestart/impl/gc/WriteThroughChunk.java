package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.bufferedOutputStream;

/**
 * A growing chunk which immediately writes added entries to the backing file.
 * <p>
 * Not thread-safe.
 */
public abstract class WriteThroughChunk extends GrowingChunk {
    final DataOutputStream dataOut;
    final FileOutputStream fileOut;
    private final GcHelper gcHelper;
    private boolean needsFsyncBeforeClosing;

    WriteThroughChunk(long seq, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, records);
        this.fileOut = out;
        this.gcHelper = gcHelper;
        this.dataOut = new DataOutputStream(bufferedOutputStream(out));
    }

    public void flagForFsyncOnClose(boolean fsyncOnClose) {
        this.needsFsyncBeforeClosing |= fsyncOnClose;
    }

    public void close() {
        if (dataOut == null) {
            return;
        }
        try {
            if (needsFsyncBeforeClosing) {
                fsync();
            }
            dataOut.close();
            gcHelper.changeSuffix(base(), seq, Chunk.FNAME_SUFFIX + ACTIVE_CHUNK_SUFFIX, Chunk.FNAME_SUFFIX);
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    public void fsync() {
        try {
            dataOut.flush();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
        fsync(fileOut);
        needsFsyncBeforeClosing = false;
    }

    final void ensureHasRoom() {
        if (full()) {
            throw new HotRestartException(String.format("Attempted to write to a full %s file #%x", base(), seq));
        }
    }

    /**
     * Writes a new record to the chunk file and updates the chunk's size.
     * May be called by the mutator thread.
     *
     * @return true if the chunk is now full.
     */
    public abstract boolean addStep1(long keyPrefix, long recordSeq, byte[] keyBytes, byte[] valBytes);

    abstract StableChunk toStableChunk();
}
