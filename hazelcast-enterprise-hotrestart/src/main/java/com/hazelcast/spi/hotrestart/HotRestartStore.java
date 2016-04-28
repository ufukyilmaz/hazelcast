package com.hazelcast.spi.hotrestart;

import com.hazelcast.spi.hotrestart.impl.ConcurrentConveyor;
import com.hazelcast.spi.hotrestart.impl.RestartItem;

/**
 * Persistent store of key-value mappings specifically tailored to support
 * the Hot Restart feature. Supports only update operations and no data lookup.
 * Data retrieval happens only during the hot restart procedure, when this
 * store pushes data to its associated RAM stores.
 * <p>
 * Update operations (put/remove/clear) accept a "needs fsync" parameter which determines
 * the perisistence semantics of the operation (strict vs. eventual). For each given key
 * prefix the value of this parameter must always be the same because the Hot Restart Store
 * will group a batch of operations by the "needs fsync" value. Order within each group is
 * preserved.
 */
public interface HotRestartStore {

    /** The name of the log category used by the Hot Restart module */
    String LOG_CATEGORY = "com.hazelcast.spi.hotrestart";

    /** Returns the store's name, which matches the name of its home directory. */
    String name();

    /**
     * Performs hot restart: reloads the data from persistent storage and
     * pushes it to its associated {@link RamStoreRegistry}.
     *
     * @param failIfAnyData if true, the call will fail if any persistent data is found
     * @param storeCount the number of Hot Restart stores associated with this HZ instance
     * @param keyConveyors convey keys from {@code HotRestartStore} to {@code RamStore}
     * @param valueConveyors convey values from {@code HotRestartStore} to {@code RamStore}
     * @param keyHandleConveyor conveys key handles from {@code RamStore} to {@code HotRestartStore}
     * @throws InterruptedException
     */
    void hotRestart(boolean failIfAnyData, int storeCount,
                    ConcurrentConveyor<RestartItem>[] keyConveyors,
                    ConcurrentConveyor<RestartItem> keyHandleConveyor,
                    ConcurrentConveyor<RestartItem>[] valueConveyors
    )
            throws InterruptedException;

    /**
     * Establishes a persistent mapping from the supplied key to the supplied value.
     * <p>
     * This method must not be called while holding a lock that can block the progress of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} on any
     * {@code RamStore} which can be returned by the {@link RamStoreRegistry}
     * associated with this Hot Restart store.
     *
     * @param needsFsync if true, the currently active chunk will be fsync'd before closing.
     *                   If this parameter is {@code false}, there is no guarantee that a later call
     *                   to {@link #fsync()} will make this operation persistent.
     * @throws HotRestartException
     */
    void put(HotRestartKey key, byte[] value, boolean needsFsync) throws HotRestartException;

    /**
     * Removes the persistent mapping for the supplied key.
     * <p>
     * This method must not be called while holding a lock that can block the progress of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} on any
     * {@code RamStore} which can be returned by the {@link RamStoreRegistry}
     * associated with this Hot Restart store.
     *
     * @param needsFsync if true, the currently active chunk will be fsync'd before closing.
     *                   If this parameter is {@code false}, there is no guarantee that a later call
     *                   to {@link #fsync()} will make this operation persistent.
     * @throws HotRestartException
     */
    void remove(HotRestartKey key, boolean needsFsync) throws HotRestartException;

    /**
     * Removes all mappings for the supplied list of key prefixes.
     * <p>
     * This method must not be called while holding a lock that can block the progress of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} on any
     * {@code RamStore} which can be returned by the {@link RamStoreRegistry}
     * associated with this Hot Restart store.
     *
     * @param needsFsync &mdash; has no effect on the semantics of persistence, but its
     *                   value must match the "needs fsync" configuration of all supplied
     *                   key prefixes
     * @param keyPrefixes The key prefixes whose data is to be cleared.
     * @throws HotRestartException
     */
    void clear(boolean needsFsync, long... keyPrefixes) throws HotRestartException;

    /**
     * Closes this Hot Restart store and releases any system resources it
     * had acquired. The store will permit no further operations on it.
     * <p>
     * This method must not be called while holding a lock that can block the progress of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} on any
     * {@code RamStore} which can be returned by the {@link RamStoreRegistry}
     * associated with this Hot Restart store.
     *
     * @throws HotRestartException
     */
    void close() throws HotRestartException;
}
