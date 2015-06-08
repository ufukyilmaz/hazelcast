package com.hazelcast.cache.wan;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Basic implementation of {@link com.hazelcast.cache.wan.CacheEntryView}
 *
 * @param <K> key
 * @param <V> value
 */
public class SimpleCacheEntryView<K, V> implements CacheEntryView<K, V>, DataSerializable {

    K key;
    V value;
    long expirationTime = CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
    long accessHit = CacheRecord.ACCESS_HIT_NOT_AVAILABLE;

    public SimpleCacheEntryView(K key, V value, long expirationTime, long accessHit) {
        this.key = key;
        this.value = value;
        this.expirationTime = expirationTime;
        this.accessHit = accessHit;
    }

    public SimpleCacheEntryView() {
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public long getAccessHit() {
        return accessHit;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
        out.writeLong(expirationTime);
        out.writeLong(accessHit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        value = IOUtil.readObject(in);
        expirationTime = in.readLong();
        accessHit = in.readLong();
    }
}
