package com.hazelcast.cache.enterprise.impl.onheap;

import com.hazelcast.cache.enterprise.EnterpriseCacheEntryProcessorEntry;
import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.nio.serialization.Data;

import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;

/**
 * @author sozal 14/10/14
 */
// TODO: Implement as on-heap
public class EnterpriseOnHeapCacheEntryProcessorEntry<K, V>
        implements EnterpriseCacheEntryProcessorEntry<K, V> {

    private K key;
    private V value;

    private State state = State.NONE;

    private final Data keyData;
    private EnterpriseOnHeapCacheRecord record;
    private EnterpriseOnHeapCacheRecord recordLoaded;

    private final EnterpriseOnHeapCacheRecordStore cacheRecordStore;
    private final long now;
    private final long start;
    private final ExpiryPolicy expiryPolicy;

    public EnterpriseOnHeapCacheEntryProcessorEntry(Data keyData,
                                                    EnterpriseOnHeapCacheRecord record,
                                                    EnterpriseOnHeapCacheRecordStore cacheRecordStore,
                                                    long now) {
        this.keyData = keyData;
        this.record = record;
        this.cacheRecordStore = cacheRecordStore;
        this.now = now;
        this.start = cacheRecordStore.cacheConfig.isStatisticsEnabled() ? System.nanoTime() : 0;

        final Factory<ExpiryPolicy> expiryPolicyFactory = cacheRecordStore.cacheConfig.getExpiryPolicyFactory();
        this.expiryPolicy = expiryPolicyFactory.create();

    }

    @Override
    public boolean exists() {
        return (record != null && state == State.NONE) || this.value != null;
    }

    @Override
    public void remove() {
        this.value = null;
        this.state = (this.state == State.CREATE || this.state == State.LOAD) ? State.NONE : State.REMOVE;
    }

    @Override
    public void setValue(V value) {
        if (value == null) {
            throw new NullPointerException("Null value not allowed");
        }
        if (this.record == null) {
            this.state = State.CREATE;
        } else {
            this.state = State.UPDATE;
        }
        this.value = value;
    }

    @Override
    public K getKey() {
        if (key == null) {
            key = (K) cacheRecordStore.cacheService.toObject(keyData);
        }
        return key;
    }

    @Override
    public V getValue() {
        if (state == State.REMOVE) {
            return null;
        }
        if (value != null) {
            return value;
        }
        if (record != null) {
            state = State.ACCESS;
            return getRecordValue(record);
        }
        if (recordLoaded == null) {
            //LOAD IT
            recordLoaded = cacheRecordStore.readThroughRecord(keyData, now);
        }
        if (recordLoaded != null) {
            state = State.LOAD;
            return getRecordValue(recordLoaded);
        }
        return null;
    }

    private V getRecordValue(EnterpriseOnHeapCacheRecord theRecord) {
        final Object objValue;
        switch (cacheRecordStore.cacheConfig.getInMemoryFormat()) {
            case BINARY:
                objValue = cacheRecordStore.cacheService.toObject(theRecord.getValue());
                break;
            case OBJECT:
                objValue = theRecord.getValue();
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + cacheRecordStore.cacheConfig.getInMemoryFormat());
        }
        return (V) objValue;
    }

    public void applyChanges() {
        final boolean isStatisticsEnabled = cacheRecordStore.cacheConfig.isStatisticsEnabled();
        final CacheStatisticsImpl statistics = cacheRecordStore.statistics;

        switch (state) {
            case ACCESS:
                cacheRecordStore.accessRecord(record, expiryPolicy, now);
                break;
            case UPDATE:
                cacheRecordStore.updateRecordWithExpiry(keyData, value, record, expiryPolicy, now, false);
                if (isStatisticsEnabled) {
                    statistics.increaseCachePuts(1);
                    statistics.addGetTimeNano(System.nanoTime() - start);
                }
                break;
            case REMOVE:
                cacheRecordStore.remove(keyData, null);
                break;
            case CREATE:
                if (isStatisticsEnabled) {
                    statistics.increaseCachePuts(1);
                    statistics.addGetTimeNano(System.nanoTime() - start);
                }
                cacheRecordStore.createRecordWithExpiry(keyData, value, expiryPolicy, now, false);
                break;
            case LOAD:
                cacheRecordStore.createRecordWithExpiry(keyData, value, expiryPolicy, now, true);
                break;
            case NONE:
                //NOOP
                break;
            default:
                break;
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    private enum State {
        NONE, ACCESS, UPDATE, LOAD, CREATE, REMOVE
    }

}
