package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.EnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.Data;

import javax.cache.expiry.ExpiryPolicy;

/**
 * <p>
 * {@link HiDensityCacheRecordStore} is the contract for Hi-Density specific cache record store operations.
 * This contract is sub-type of {@link ICacheRecordStore} and used by Hi-Density cache based operations.
 * </p>
 * <p>
 * This {@link HiDensityCacheRecordStore} mainly manages operations for
 * {@link HiDensityCacheRecord} like get, put, replace, remove, iterate, etc ...
 * </p>
 *
 * @param <R> Type of the cache record to be stored
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore
 * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
 *
 * @author sozal 18/10/14
 */
public interface HiDensityCacheRecordStore<R extends HiDensityCacheRecord>
        extends EnterpriseCacheRecordStore {

    /**
     * Constant value for default forced eviction percentage
     */
    int DEFAULT_FORCED_EVICT_PERCENTAGE = 20;

    /**
     * Constant value for representing the empty address
     */
    long NULL_PTR = MemoryManager.NULL_ADDRESS;

    /**
     * Constant for system property name for enabling/disabling throwing exception behaviour
     * when invalid max-size policy config
     */
    String SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION =
            "disableInvalidMaxSizePolicyException";

    /**
     * Gets the value of specified cache record.
     *
     * @param record The record whose value is extracted
     * @return value of the record
     */
    Object getRecordValue(R record);

    /**
     * Gets the {@link MemoryManager} which is used by this {@link HiDensityCacheRecordStore}.
     *
     * @return the used memory manager.
     */
    MemoryManager getMemoryManager();

    /**
     * Gets the {@link HiDensityCacheRecordProcessor}
     * which is used by this {@link HiDensityCacheRecordStore}.
     *
     * @return the used Hi-Density cache record processor.
     */
    HiDensityCacheRecordProcessor<R> getCacheRecordProcessor();

    /**
     * Puts the <code>value</code> with the specified <code>key</code> to this {@link HiDensityCacheRecordStore}.
     * without sending any events
     *
     * @param key          the key of the <code>value</code> to be put
     * @param value        the value to be put
     * @param expiryPolicy expiry policy of the record
     * @return <code>true</code> if the <code>value</code> has been added as new record,
     * otherwise (in case of update) <code>false</code>
     */
    boolean putBackup(Data key, Object value, ExpiryPolicy expiryPolicy);

    /**
     * Owns and saves (replaces if exist) the replicated <code>value</code> with the specified <code>key</code>
     * to this {@link HiDensityCacheRecordStore}.
     *
     * @param key       the key of the <code>value</code> to be owned
     * @param value     the value to be owned
     * @param ttlMillis the TTL value in milliseconds for the owned value
     * @return <code>true</code> if the <code>value</code> has been added as new record,
     * otherwise (in case of update) <code>false</code>
     */
    boolean own(Data key, Object value, long ttlMillis);

    /**
     * Puts the <code>value</code> with the specified <code>key</code>
     * to this {@link HiDensityCacheRecordStore} if there is no value with the same key.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     * @return <code>true</code> if the <code>value</code> has been put to the record store,
     * otherwise <code>false</code>
     */
    boolean putIfAbsent(Data key, Object value, String caller, int completionId);

    /**
     * Replaces the already stored value with the new <code>value</code>
     * mapped by the specified <code>key</code> to this {@link HiDensityCacheRecordStore}
     * if there is a value with the specified <code>key</code>.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     * @return <code>true</code> if the <code>value</code> has been replaced with the specified <code>value</code>,
     * otherwise <code>false</code>
     */
    boolean replace(Data key, Object value, String caller, int completionId);

    /**
     * Replaces the already stored value with the new <code>value</code>
     * mapped by the specified <code>key</code> to this {@link HiDensityCacheRecordStore}
     * if there is a value with the specified <code>key</code> and equals to specified <code>value</code>.
     *
     * @param key      the key of the <code>value</code> to be put
     * @param oldValue the expected value of the record for the specified <code>key</code>
     * @param newValue the new value to be put
     * @param caller   the id which represents the caller
     * @return <code>true</code> if the <code>value</code> has been replaced with the specified <code>value</code>,
     * otherwise <code>false</code>
     */
    boolean replace(Data key, Object oldValue, Object newValue, String caller, int completionId);

    /**
     * Replaces the already stored value with the new <code>value</code>
     * mapped by the specified <code>key</code> to this {@link HiDensityCacheRecordStore}
     * if there is a value with the specified <code>key</code> and
     * returns the old value of the replaced record if exist.
     *
     * @param key    the key of the <code>value</code> to be put
     * @param value  the value to be put
     * @param caller the id which represents the caller
     * @return the old value of the record with the specified <code>key</code> if exist, otherwise <code>null</code>
     */
    Object getAndReplace(Data key, Object value, String caller, int completionId);

    /**
     * Returns an slottable iterator for this {@link HiDensityCacheRecordStore} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @param <E>  the type of the entry iterated by the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    <E> SlottableIterator<E> iterator(int slot);

    /**
     * Forcefully evict records.
     *
     * @return evicted entry count
     */
    int forceEvict();

}
