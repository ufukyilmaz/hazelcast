package com.hazelcast.spi.hotrestart;

/**
 * Identifies a record in the Hot Restart store. This is a
 * "heavyweight" key which contains the raw bytes representing
 * the client's ID of a record. It wraps the "lightweight" variant,
 * {@link KeyHandle}, which only contains the minimum data needed
 * to look up a record within Hot Restart store's metadata structures.
 */
public interface HotRestartKey {
    /**
     * @return the key prefix of this key.
     */
    long prefix();

    /**
     * @return raw bytes representing the client's ID
     */
    byte[] bytes();

    /**
     * @return the lightweight key handle the garbage collector uses
     * to identify the key with minimum RAM overhead.
     */
    KeyHandle handle();
}
