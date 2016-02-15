package com.hazelcast.elastic.map;

import com.hazelcast.nio.Disposable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @param <K> key type
 * @param <V> value type
 * @author mdogan 07/01/14
 */
public interface ElasticMap<K, V> extends Map<K, V>, Disposable {

    V put(K key, V value);

    /**
     * @param key
     * @param value
     * @return true if first put, false if updated
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

    Set<Entry<K, V>> entrySet();

    int size();

    boolean isEmpty();

    void clear();
}
