package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author mdogan 15/05/14
 */
public abstract class AbstractCacheIterator<K, V> implements Iterator<Cache.Entry<K, V>> {

    protected final ICache<K, V> cache;
    protected final int batchCount;

    protected int partitionId = -1;
    protected int lastSlot = -1;

    private CacheIterationResult result;
    private int index;
    private int currentIndex = -1;

    protected AbstractCacheIterator(ICache<K, V> cache, int batchCount) {
        this.cache = cache;
        this.batchCount = batchCount;
        advance();
    }

    private boolean advance() {
        int partitionCount = getPartitionCount();
        while (partitionId < partitionCount) {
            if (result == null || result.getCount() < batchCount || lastSlot < 0) {
                partitionId++;
                lastSlot = 0;
                result = null;
                if (partitionId == partitionCount) {
                    return false;
                }
            }
            result = fetch();
            if (result != null && result.getCount() > 0) {
                index = 0;
                lastSlot = result.getSlot();
                return true;
            }
        }
        return false;
    }

    protected abstract CacheIterationResult fetch();

    protected abstract int getPartitionCount();

    @Override
    public boolean hasNext() {
        if (result != null && index < result.getCount()) {
            return true;
        }
        return advance();
    }

    @Override
    public Cache.Entry<K, V> next() {
        if (result == null || index >= result.getCount()) {
            throw new NoSuchElementException();
        }
        currentIndex = index;
        index++;
        Data key = result.getKey(currentIndex);
        Data value = result.getValue(currentIndex);
        return new CacheEntry(key, value);
    }

    @Override
    public void remove() {
        if (result == null || currentIndex < 0) {
            throw new IllegalStateException("Iterator.next() must be called before remove()!");
        }
        Data dataKey = result.getKey(currentIndex);
        K key = getSerializationService().toObject(dataKey);
        cache.remove(key);
        currentIndex = -1;
    }

    private class CacheEntry implements Cache.Entry<K, V> {

        final Data key;
        final Data value;

        private CacheEntry(Data key, Data value) {
            if (key == null || key.dataSize() <= 0) {
                throw new NullPointerException("Key is null! " + key + " -> " + result);
            }
            if (value == null || value.dataSize() <= 0) {
                throw new NullPointerException("Value is null! " + value + " -> " + result);
            }
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return getSerializationService().toObject(key);
        }

        @Override
        public V getValue() {
            return getSerializationService().toObject(value);
        }

        @Override
        public CacheEntry unwrap(Class clazz) {
            if (clazz == CacheEntry.class) {
                return this;
            }
            throw new IllegalArgumentException();
        }
    }

    protected abstract SerializationService getSerializationService();
}
