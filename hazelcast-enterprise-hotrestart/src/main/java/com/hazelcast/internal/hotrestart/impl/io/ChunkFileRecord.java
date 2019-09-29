package com.hazelcast.internal.hotrestart.impl.io;

/**
 * Represents a record read from a chunk file.
 */
public interface ChunkFileRecord {

    /** @return chunk's seq */
    long chunkSeq();

    /** @return record's seq */
    long recordSeq();

    /** @return record's key prefix */
    long prefix();

    /** @return record's position in the file */
    int filePos();

    /** @return number of bytes the record occupies in the file */
    int size();

    /** @return record's key blob */
    byte[] key();

    /** @return record's value blob */
    byte[] value();
}
