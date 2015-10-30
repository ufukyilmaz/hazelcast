package com.hazelcast.spi.hotrestart;

/**
 * Persistent store of key-value mappings specifically tailored to support
 * the Hot Restart feature. Supports only update operations and no data lookup.
 * Data retrieval happens only during the hot restart procedure, when this
 * store pushes data to its associated RAM stores.
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
    void hotRestart(boolean failIfAnyData);

    /**
     * Establishes a persistent mapping from the supplied key to the supplied value.
     * @throws HotRestartException
     */
    void put(HotRestartKey key, byte[] value) throws HotRestartException;

    /**
     * Step one of the two-step removal idiom. Prepares to remove the mapping
     * for the supplied key from the Hot Restart store.
     * <p>
     * Must be directly followed by a call of {@link #removeStep2()}.
     * @return Sequence ID of the associated tombstone, which will be
     * referred to in a later call to {@link RamStore#releaseTombstones(java.util.Collection)}.
     * @throws HotRestartException
     */
    long removeStep1(HotRestartKey key) throws HotRestartException;

    /**
     * Completes the two-step removal idiom. When this call returns
     * the mapping for the supplied has been removed from the Hot Restart store.
     * <p>
     * Must directly follow a call of {@link #removeStep1(HotRestartKey)}.
     * Must not be called while holding a lock that can block any
     * operation on any of the {@link RamStore}s that can be returned
     * by the {@link RamStoreRegistry} associated with this Hot Restart store.
     * @throws HotRestartException
     */
    void removeStep2() throws HotRestartException;

    /**
     * Removes all mappings for the supplied list of key prefixes.
     * <p>
     * The completion of this method must <i>happen-before</i> any call of
     * {@link RamStore#copyEntry(KeyHandle, int, RecordDataSink)} which
     * observes the effects of the clear operation. Must not be called
     * while holding a lock that can block any operation on any of the
     * {@link RamStore}s that can be returned by the {@link RamStoreRegistry}
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
     * Sets whether auto-fsync is enabled for this Hot Restart store.
     * When enabled, at the completion of each call to {@link #put(HotRestartKey, byte[])},
     * {@link #removeStep2()}, and {@link #clear(long...)} there is a guarantee
     * that its effects have become persistent.
     */
    void setAutoFsync(boolean autoFsync);

    /**
     * @return whether auto-fsync is enabled for this Hot Restart store.
     */
    boolean isAutoFsync();

    /**
     * When this method completes it is guaranteed that the effects of all preceding
     * calls to {@link #put(HotRestartKey, byte[])}, {@link #removeStep2()}, and
     * {@link #clear(long...)} have become persistent.
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
     * @throws HotRestartException
     */
    void close() throws HotRestartException;
}
