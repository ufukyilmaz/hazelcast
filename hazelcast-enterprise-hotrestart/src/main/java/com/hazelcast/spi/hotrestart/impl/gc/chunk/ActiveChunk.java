package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.nio.Disposable;

/**
 * Represents a currently active chunk. The mutator adds records to this chunk. Associated with an
 * open file.
 */
public interface ActiveChunk extends Disposable {

    /**
     * Writes a record to the chunk file and updates the {@code size} property on this object.
     * May be called on the mutator thread.
     *
     * @return {@code true} if the chunk is now full.
     */
    boolean addStep1(long recordSeq, long keyPrefix, byte[] keyBytes, byte[] valueBytes);

    /**
     * @return size in bytes of the chunk file
     */
    long size();

    /**
     * As soon as a record which "needs fsync" is added to the chunk, it must not be closed
     * without fsync-ing it first. Calling this method with {@code true} signals that need.
     * @param needsFsync {@code true} if the chunk will need an {@code fsync} before closing.
     */
    void flagForFsyncOnClose(boolean needsFsync);

    /**
     * Performs the {@code fsync} operation on the open chunk file.
     */
    void fsync();

    void close();

    /**
     * Converts this active chunk into a stable chunk.
     */
    StableChunk toStableChunk();
}
