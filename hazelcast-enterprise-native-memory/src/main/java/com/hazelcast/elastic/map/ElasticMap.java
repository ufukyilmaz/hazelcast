package com.hazelcast.elastic.map;

import com.hazelcast.nio.Disposable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Extension of {@link java.util.Map} which additionally supports {@code set} and {@code delete} operations,
 * which are like {@code put} and {@code remove} but don't return the previous value. When methods are
 * called over an RPC mechanism that must serialize/deserialize return values, using these variants whenever
 * the return value isn't needed can improve performance.
 *
 * @param <K> key type
 * @param <V> value type
 * @author mdogan 07/01/14
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

    V putIfAbsent(K key, V value);

    boolean replace(K key, V oldValue, V newValue);

    V replace(K key, V value);

    boolean remove(final Object key, final Object value);
}
