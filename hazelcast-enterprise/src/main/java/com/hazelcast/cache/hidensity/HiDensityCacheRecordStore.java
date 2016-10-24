package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.EnterpriseCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityRecordStore;
import com.hazelcast.nio.serialization.Data;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;

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
 * @see com.hazelcast.cache.hidensity.HiDensityCacheRecordStore
 * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
 *
 * @author sozal 18/10/14
 */
public interface HiDensityCacheRecordStore<R extends HiDensityCacheRecord>
        extends HiDensityRecordStore<R>, EnterpriseCacheRecordStore {

    /**
     * Constant for system property name for enabling/disabling throwing exception behaviour
     * when invalid max-size policy config
     */
    String SYSTEM_PROPERTY_NAME_TO_DISABLE_INVALID_MAX_SIZE_POLICY_EXCEPTION =
            "disableInvalidMaxSizePolicyException";

    /**
     * Gets the value of specified record.
     *
     * @param record The record whose value is extracted
     * @return value of the record
     */
    Object getRecordValue(R record);

    /**
     * Gets the {@link HiDensityRecordProcessor}
     * which is used by this {@link HiDensityCacheRecordStore}.
     *
     * @return the used Hi-Density cache record processor.
     */
    HiDensityRecordProcessor<R> getRecordProcessor();

    /**
     * Puts and saves (replaces if exist) the backup <code>value</code> with the specified <code>key</code>
     * to this {@link HiDensityCacheRecordStore}.
     *
     * @param key           the key of the <code>value</code> to be owned
     * @param value         the value to be owned
     * @param expiryPolicy  expiry policy of the owned value
     * @return <code>true</code> if the <code>value</code> has been added as new record,
     * otherwise (in case of update) <code>false</code>
     */
    boolean putBackup(Data key, Object value, ExpiryPolicy expiryPolicy);

    /**
     * Puts and saves (replaces if exist) the replicated <code>value</code> with the specified <code>key</code>
     * to this {@link HiDensityCacheRecordStore}.
     *
     * @param key       the key of the <code>value</code> to be owned
     * @param value     the value to be owned
     * @param ttlMillis the TTL value in milliseconds for the owned value
     * @return <code>true</code> if the <code>value</code> has been added as new record,
     * otherwise (in case of update) <code>false</code>
     */
    boolean putReplica(Data key, Object value, long ttlMillis);

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
     * Returns a slottable iterator for this {@link HiDensityCacheRecordStore} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    SlottableIterator<Map.Entry<Data, R>> iterator(int slot);

    /**
     * Converts given object to data to be sent inside event
     *
     * @param obj the object to be converted to data to be sent inside event
     * @return the data to be sent
     */
    Data toEventData(Object obj);

    /**
     * Converts given {@link HiDensityCacheRecord} to heap based {@link CacheRecord}
     *
     * @param record the {@link HiDensityCacheRecord} to be converted to heap based {@link CacheRecord}
     * @return the heap based {@link CacheRecord} converted from the given {@link HiDensityCacheRecord}
     */
    CacheRecord toHeapCacheRecord(R record);

}
