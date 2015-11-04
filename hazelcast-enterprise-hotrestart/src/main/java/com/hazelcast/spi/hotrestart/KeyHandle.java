package com.hazelcast.spi.hotrestart;

/**
 * A "lightweight" key which uniquely identifies a record
 * in the Hot Restart store's metadata structures using the minimal
 * amount of data.
 * <p>
 * The key handle is also a point of understanding between the
 * garbage collector and the RAM store, which can look up a record
 * based on it.
 */
public interface KeyHandle {
}
