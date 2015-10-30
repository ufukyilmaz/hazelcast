package com.hazelcast.spi.hotrestart;

import java.nio.ByteBuffer;

/**
 * A mutable sink for the data of a single record in the Hot Restart
 * Store. Used to transfer data from the in-memory store to chunk files
 * during a GC cycle.
 */
public interface RecordDataSink {
    /**
     * Provides a <code>ByteBuffer</code> into which the key data should
     * be put. The buffer will have sufficient <code>remaining()</code>
     * bytes to accommodate the requested key size.
     * @param keySize size of the record's key
     */
    ByteBuffer getKeyBuffer(int keySize);
    /**
     * Provides a <code>ByteBuffer</code> into which the value data should
     * be put. The buffer will have sufficient <code>remaining()</code>
     * bytes to accommodate the requested value size.
     * @param valueSize size of the record's value
     */
    ByteBuffer getValueBuffer(int valueSize);
}
