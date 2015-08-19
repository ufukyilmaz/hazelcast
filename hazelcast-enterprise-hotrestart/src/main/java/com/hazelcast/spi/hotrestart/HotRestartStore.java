package com.hazelcast.spi.hotrestart;

/**
 * Not thread-safe.
 */
public interface HotRestartStore {

    void hotRestart();

    void put(long prefix, byte[] key, byte[] value) throws HotRestartException;

    void putNoRetain(KeyHandle keyHandle, byte[] keyBytes, byte[] value) throws HotRestartException;

    void remove(long prefix, byte[] keyBytes) throws HotRestartException;

    void remove(KeyHandle keyHandle, byte[] keyBytes) throws HotRestartException;

    String name();

    boolean isEmpty();

    void clear() throws HotRestartException;

    void close() throws HotRestartException;

    void destroy() throws HotRestartException;
}
