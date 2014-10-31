package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.Data;

/**
 * <p>
 * {@link BreakoutCacheRecordStore} is the contract for Hi-Density specific cache record store operations.
 * This contract is sub-type of {@link ICacheRecordStore} and used by Hi-Density cache based operations.
 * </p>
 * <p>
 * This {@link BreakoutCacheRecordStore} mainly manages operations for
 * {@link BreakoutCacheRecord} like get, put, replace, remove, iterate, etc ...
 * </p>
 *
 * @param <R> Type of the cache record to be stored
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore
 * @see com.hazelcast.cache.enterprise.impl.nativememory.BreakoutNativeMemoryCacheRecordStore
 *
 * @author sozal 18/10/14
 */
public interface BreakoutCacheRecordStore<R extends BreakoutCacheRecord>
        extends ICacheRecordStore {

    /**
     * Constant value for representing the empty address
     */
    long NULL_PTR = MemoryManager.NULL_ADDRESS;

    /**
     * Gets the value of specified cache record.
     *
     * @param record The record whose value is extracted
     * @return value of the record
     */
    Object getRecordValue(R record);

    /**
     * Gets the {@link MemoryManager} which is used by this {@link BreakoutCacheRecordStore}.
     *
     * @return the used memory manager.
     */
    MemoryManager getMemoryManager();

    /**
     * Gets the {@link BreakoutCacheRecordAccessor}
     * which is used by this {@link BreakoutCacheRecordStore}.
     *
     * @return the used Hi-Density cache record accessor.
     */
    BreakoutCacheRecordAccessor<R> getCacheRecordAccessor();

    /**
     * Puts the <code>value</code> with the specified <code>key</code> to this {@link BreakoutCacheRecordStore}.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     */
    void put(Data key, Object value, String caller);

    /**
     * Owns and saves (replaces if exist) the replicated <code>value</code> with the specified <code>key</code>
     * to this {@link BreakoutCacheRecordStore}.
     *
     * @param key       the key of the <code>value</code> to be owned
     * @param value     the value to be owned
     * @param ttlMillis the TTL value in milliseconds for the owned value
     */
    void own(Data key, Object value, long ttlMillis);

    /**
     * Puts the <code>value</code> with the specified <code>key</code>
     * to this {@link BreakoutCacheRecordStore} if there is no value with the same key.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     * @return <code>true</code> if the <code>value</code> has been put to the record store,
     * otherwise <code>false</code>
     */
    boolean putIfAbsent(Data key, Object value, String caller);

    /**
     * Replaces the already stored value with the new <code>value</code>
     * mapped by the specified <code>key</code> to this {@link BreakoutCacheRecordStore}
     * if there is a value with the specified <code>key</code>.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     * @return <code>true</code> if the <code>value</code> has been replaced with the specified <code>value</code>,
     * otherwise <code>false</code>
     */
    boolean replace(Data key, Object value, String caller);

    /**
     * Replaces the already stored value with the new <code>value</code>
     * mapped by the specified <code>key</code> to this {@link BreakoutCacheRecordStore}
     * if there is a value with the specified <code>key</code> and equals to specified <code>value</code>.
     *
     * @param key      the key of the <code>value</code> to be put
     * @param oldValue the expected value of the record for the specified <code>key</code>
     * @param newValue the new value to be put
     * @param caller   the id which represents the caller
     * @return <code>true</code> if the <code>value</code> has been replaced with the specified <code>value</code>,
     * otherwise <code>false</code>
     */
    boolean replace(Data key, Object oldValue, Object newValue, String caller);

    /**
     * Replaces the already stored value with the new <code>value</code>
     * mapped by the specified <code>key</code> to this {@link BreakoutCacheRecordStore}
     * if there is a value with the specified <code>key</code> and
     * returns the old value of the replaced record if exist.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     * @return the old value of the record with the specified <code>key</code> if exist, otherwise <code>null</code>
     */
    Object getAndReplace(Data key, Object value, String caller);

    /**
     * Returns an slottable iterator for this {@link BreakoutCacheRecordStore} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @param <E>  the type of the entry iterated by the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    <E> SlottableIterator<E> iterator(int slot);

}
