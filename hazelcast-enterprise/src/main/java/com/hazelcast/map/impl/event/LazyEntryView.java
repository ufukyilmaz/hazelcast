package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * LazyEntryView is an implementation of {@link
 * com.hazelcast.core.EntryView} and also it is writable.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */

class LazyEntryView<K, V> implements EntryView<K, V> {
    private K key;
    private V value;
    private long cost;
    private long creationTime;
    private long expirationTime;
    private long hits;
    private long lastAccessTime;
    private long lastStoredTime;
    private long lastUpdateTime;
    private long version;
    private long ttl;
    private Long maxIdle;

    private SerializationService serializationService;

    LazyEntryView() {
    }

    LazyEntryView(K key, V value, SerializationService serializationService) {
        this.value = value;
        this.key = key;
        this.serializationService = serializationService;
    }

    @Override
    public K getKey() {
        key = serializationService.toObject(key);
        return key;
    }

    @Override
    public V getValue() {
        value = serializationService.toObject(value);
        return value;
    }

    @Override
    public long getCost() {
        return cost;
    }

    public LazyEntryView<K, V> setCost(long cost) {
        this.cost = cost;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public LazyEntryView<K, V> setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    public LazyEntryView<K, V> setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public LazyEntryView<K, V> setHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public LazyEntryView<K, V> setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public LazyEntryView<K, V> setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public LazyEntryView<K, V> setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public LazyEntryView<K, V> setVersion(long version) {
        this.version = version;
        return this;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    public LazyEntryView<K, V> setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public Long getMaxIdle() {
        return maxIdle;
    }

    public LazyEntryView<K, V> setMaxIdle(Long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }
}