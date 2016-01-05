package com.hazelcast.spi.hotrestart;

/**
 * Persistent store of key-value mappings specifically tailored to support
 * the Hot Restart feature. Supports only update operations and no data lookup.
 * Data retrieval happens only during the hot restart procedure, when this
 * store pushes data to its associated RAM stores.
 * <p>
 * The store supports a batch operating mode where a number of updates are fsync'd
 * to the file system together instead of one by one. To achieve this,
 * <ol>
 *     <li>call {@link #fsync()} when all the updates in a batch are done;</li>
 *     <li>when calling {@link #put(HotRestartKey, byte[], boolean)} and {@link #remove(HotRestartKey, boolean)},
 *     make sure you pass {@code true} for the {@code boolean needsFsync} parameter to ensure that all chunk files
 *     that were active at some point during the batch operation are fsync'd before closing.</li>
 * </ol>
 * <p>
 * This class is not thread-safe. The caller must ensure a <i>happens-before</i>
 * relationship between any two method calls.
 */
public interface HotRestartStore {

    /** The name of the log category used by the Hot Restart module */
    String LOG_CATEGORY = "com.hazelcast.spi.hotrestart";

    /**
     * Performs hot restart: reloads the data from persistent storage and
     * pushes it to its associated {@link RamStoreRegistry}.
     *
     * @param failIfAnyData if true, the call will fail if any persistent data is found.
     */
    void hotRestart(boolean failIfAnyData) throws InterruptedException;

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
     * The completion of this method must <i>happen-before</i> any call of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} which
     * observes the effects of the clear operation on the RAM store.
     * <p>
     * This method must not be called while holding a lock that can block the progress of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} on any
     * {@code RamStore} which can be returned by the {@link RamStoreRegistry}
     * associated with this Hot Restart store.
     *
     * @param keyPrefixes The key prefixes whose data is to be cleared.
     * @throws HotRestartException
     */
    void clear(long... keyPrefixes) throws HotRestartException;

    /**
     * @return the name of this Hot Restart store
     */
    String name();

    /**
     * When this method completes, it is guaranteed that the effects of all preceding
     * calls to {@link #put(HotRestartKey, byte[], boolean)}, {@link #remove(HotRestartKey, boolean)},
     * and {@link #clear(long...)} have become persistent. Note that calls to {@code put} and {@code remove}
     * with {@code needsFsync == false} are excluded from this guarantee.
     */
    void fsync();

    /**
     * Reports whether this Hot Restart store is completely empty. The store is
     * empty when it contains neither live nor dead (garbage) records. Usually
     * this is the case only before the first update operation on it.
     */
    boolean isEmpty();

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
