package com.hazelcast.cache.wan;

import com.hazelcast.cache.impl.record.CacheRecord;

/**
 * Helper class to create {@link com.hazelcast.cache.wan.CacheEntryView} instances
 */
public final class CacheEntryViews {

    private CacheEntryViews() {
    }

    public static <K, V> CacheEntryView<K, V> createSimpleCacheEntryView() {
        return new SimpleCacheEntryView<K, V>();
    }

    public static <K, V> CacheEntryView<K, V> createSimpleCacheEntryView(K key, V value, CacheRecord record) {
        final SimpleCacheEntryView simpleCacheEntryView = new SimpleCacheEntryView(key, value);
        simpleCacheEntryView.setAccessHit(record.getAccessHit());
        simpleCacheEntryView.setExpirationTime(record.getExpirationTime());
        return simpleCacheEntryView;
    }
}
