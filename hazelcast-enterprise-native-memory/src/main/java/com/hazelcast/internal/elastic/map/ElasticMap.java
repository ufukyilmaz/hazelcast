package com.hazelcast.internal.elastic.map;

import com.hazelcast.internal.nio.Disposable;

import java.util.Map;

/**
 * Extension of {@link java.util.Map} which additionally supports {@code set} and {@code delete} operations,
 * which are like {@code put} and {@code remove} but don't return the previous value. When methods are
 * called over an RPC mechanism that must serialize/deserialize return values, using these variants whenever
 * the return value isn't needed can improve performance.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface ElasticMap<K, V> extends Map<K, V>, Disposable {

    /**
     * Like {@link #put(Object, Object)}, but doesn't return the previously mapped value.
     *
     * @return {@code true} there was no mapping for {@code key}, {@code false} otherwise
     */
    boolean set(K key, V value);

    /**
     * Like {@link #remove(Object)}, but doesn't return the previously mapped value.
     *
     * @return {@code true} if a mapping for {@code key} existed and was removed, {@code false} otherwise
     */
    boolean delete(K key);

    // backport of Java 8 methods

    V putIfAbsent(K key, V value);

    boolean replace(K key, V oldValue, V newValue);

    V replace(K key, V value);

    boolean remove(Object key, Object value);
}
