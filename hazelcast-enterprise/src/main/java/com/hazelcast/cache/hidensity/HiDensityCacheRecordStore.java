package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.hidensity.impl.HiDensityCacheRecord;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.elastic.SlottableIterator;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.nio.serialization.Data;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;

/**
 * {@link HiDensityCacheRecordStore} is the contract for Hi-Density specific cache record store operations.
 * This contract is sub-type of {@link ICacheRecordStore} and used by Hi-Density cache based operations.
 * <p>
 * This {@link HiDensityCacheRecordStore} mainly manages operations for
 * {@link HiDensityCacheRecord} like get, put, replace, remove, iterate, etc.
 *
 * @param <R> Type of the cache record to be stored.
 * @see com.hazelcast.cache.hidensity.HiDensityCacheRecordStore
 * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
 */
public interface HiDensityCacheRecordStore<R extends HiDensityCacheRecord>
        extends HiDensityRecordStore<R>, ICacheRecordStore {

    /**
     * Constant for system property name for enabling/disabling throwing exception behavior
     * in the case of an invalid max-size policy configuration.
     */
    String SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION =
            "disableInvalidMaxSizePolicyException";

    /**
     * Gets the value of specified record.
     *
     * @param record the record whose value is extracted
     * @return value of the record
     */
    Object getRecordValue(R record);

    /**
     * Gets the {@link HiDensityRecordProcessor}
     * which is used by this {@link HiDensityCacheRecordStore}.
     *
     * @return the used Hi-Density cache record processor
     */
    HiDensityRecordProcessor<R> getRecordProcessor();

    /**
     * Puts and saves (replaces if exist) the backup {@code value} with the specified {@code key}
     * to this {@link HiDensityCacheRecordStore}.
     *
     * @param key          the key of the {@code value} to be owned
     * @param value        the value to be owned
     * @param expiryPolicy expiry policy of the owned value
     * @return the updated (or added) record
     */
    CacheRecord putBackup(Data key, Object value, long creationTime, ExpiryPolicy expiryPolicy);

    /**
     * Puts and saves (replaces if exist) the replicated {@code value} with the specified {@code key}
     * to this {@link HiDensityCacheRecordStore}.
     *
     * @param key       the key of the {@code value} to be owned
     * @param value     the value to be owned
     * @param ttlMillis the TTL value in milliseconds for the owned value
     * @return the updated (or added) record
     */
    CacheRecord putReplica(Data key, Object value, long creationTime, long ttlMillis);

    /**
     * Puts the {@code value} with the specified {@code key}
     * to this {@link HiDensityCacheRecordStore} if there is no value with the same key.
     *
     * @param key    the key of the {@code value} to be put
     * @param value  the value to be put
     * @param caller the ID which represents the caller
     * @return {@code true} if the {@code value} has been put to the record store,
     * otherwise {@code false}
     */
    boolean putIfAbsent(Data key, Object value, String caller, int completionId);

    /**
     * Replaces the already stored value with the new {@code value}
     * mapped by the specified {@code key} to this {@link HiDensityCacheRecordStore}
     * if there is a value with the specified {@code key}.
     *
     * @param key    the key of the {@code value} to be put
     * @param value  the value to be put
     * @param caller the ID which represents the caller
     * @return {@code true} if the {@code value} has been replaced with the specified {@code value},
     * otherwise {@code false}
     */
    boolean replace(Data key, Object value, String caller, int completionId);

    /**
     * Replaces the already stored value with the new {@code value}
     * mapped by the specified {@code key} to this {@link HiDensityCacheRecordStore}
     * if there is a value with the specified {@code key} and equals to specified {@code value}.
     *
     * @param key      the key of the {@code value} to be put
     * @param oldValue the expected value of the record for the specified {@code key}
     * @param newValue the new value to be put
     * @param caller   the ID which represents the caller
     * @return {@code true} if the {@code value} has been replaced with the specified {@code value},
     * otherwise {@code false}
     */
    boolean replace(Data key, Object oldValue, Object newValue, String caller, int completionId);

    /**
     * Returns a slottable iterator for this {@link HiDensityCacheRecordStore} to iterate over records.
     *
     * @param slot the slot number (or index) to start the {@code iterator}
     * @return the slottable iterator for specified {@code slot}
     */
    SlottableIterator<Map.Entry<Data, R>> iterator(int slot);

    /**
     * Converts the given object to data to be sent inside event.
     *
     * @param obj the object to be converted to data to be sent inside event
     * @return the data to be sent
     */
    Data toEventData(Object obj);

    /**
     * Converts given {@link HiDensityCacheRecord} to heap based {@link CacheRecord}.
     *
     * @param record the {@link HiDensityCacheRecord} to be converted to heap based {@link CacheRecord}
     * @return the heap based {@link CacheRecord} converted from the given {@link HiDensityCacheRecord}
     */
    CacheRecord toHeapCacheRecord(R record);

    /**
     * Used to release HD memory.
     */
    void disposeDeferredBlocks();
}
