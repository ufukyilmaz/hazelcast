package com.hazelcast.elastic.map;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author mdogan 07/01/14
 */
public interface ElasticMap<K, V> extends Map<K, V> {

    /**
     * Maximum capacity for an array that is of power-of-two size and still
     * allocatable in Java (not a negative int).
     */
    int MAX_CAPACITY = 0x80000000 >>> 1;

    /**
     * Minimum capacity for a hash container.
     */
    int MIN_CAPACITY = 4;

    /**
     * Default capacity for a hash container.
     */
    int DEFAULT_CAPACITY = 16;

    /**
     * Default load factor.
     */
    float DEFAULT_LOAD_FACTOR = 0.91f;


    V put(K key, V value);

    /**
     * @param key
     * @param value
     * @return true if updated, false if first put
     */
    boolean set(K key, V value);

    V putIfAbsent(K key, V value);

    boolean replace(K key, V oldValue, V newValue);

    V replace(K key, V value);

    V remove(Object key);

    boolean delete(K key);

    boolean remove(final Object key, final Object value);

    V get(Object key);

    boolean containsKey(Object key);

    boolean containsValue(final Object value);

    Set<K> keySet();

    Collection<V> values();

    Set<Map.Entry<K, V>> entrySet();

    int size();

    boolean isEmpty();

    void clear();

    void destroy();
}
