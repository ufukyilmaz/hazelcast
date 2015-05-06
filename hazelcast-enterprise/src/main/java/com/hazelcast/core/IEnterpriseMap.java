package com.hazelcast.core;

import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.annotation.Beta;

/**
 * Contains enterprise extensions for {@link IMap} interface.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see IMap
 * @since 3.5
 */
@Beta
public interface IEnterpriseMap<K, V> extends IMap<K, V> {

    /**
     * Returns corresponding {@code QueryCache} instance for the supplied {@code name} or null.
     * <p/>
     * If there is a previously created {@link QueryCache} with the supplied {@code name} or if there is a declarative
     * configuration exists for the supplied {@code name} this method returns or creates the instance respectively,
     * otherwise returns null.
     *
     * @param name the name of {@code QueryCache}
     * @return the {@code QueryCache} instance or null if there is no corresponding {@code QueryCacheConfig}
     * @throws NullPointerException if the specified {@code name} is null.
     * @see QueryCache
     * @since 3.5
     */
    QueryCache<K, V> getQueryCache(String name);

    /**
     * Creates an always up to date snapshot of this {@code IEnterpriseMap} according to the supplied parameters.
     * <p/>
     * If there is a previously created {@link QueryCache} with the supplied {@code name}, this method returns that
     * {@link QueryCache} and ignores {@code predicate} and {@code includeValue} parameters. Otherwise it creates and returns
     * a new {@link QueryCache} instance.
     * <p/>
     * Also note that if there exists a {@link com.hazelcast.config.QueryCacheConfig QueryCacheConfig} for the supplied
     * {@code name}, {@code predicate} and {@code includeValue} parameters will overwrite corresponding ones
     * in {@link com.hazelcast.config.QueryCacheConfig}.
     *
     * @param name         the name of {@code QueryCache}
     * @param predicate    the predicate for filtering entries.
     * @param includeValue {@code true} if this {@code QueryCache} is allowed to cache values of entries, otherwise {@code false}
     * @return the {@code QueryCache} instance with the supplied {@code name}
     * @throws NullPointerException if the specified {@code name} or {@code predicate} is null.
     * @see QueryCache
     * @since 3.5
     */
    QueryCache<K, V> getQueryCache(String name, Predicate<K, V> predicate, boolean includeValue);

    /**
     * Creates an always up to date snapshot of this {@code IEnterpriseMap} according to the supplied parameters.
     * <p/>
     * If there is a previously created {@link QueryCache} with the supplied {@code name}, this method returns that
     * {@link QueryCache} and ignores {@code listener}, {@code predicate} and {@code includeValue} parameters.
     * Otherwise it creates and returns a new {@link QueryCache} instance.
     * <p/>
     * Also note that if there exists a {@link com.hazelcast.config.QueryCacheConfig QueryCacheConfig} for the supplied
     * {@code name}, {@code listener},{@code predicate} and {@code includeValue} parameters will overwrite corresponding ones
     * in {@link com.hazelcast.config.QueryCacheConfig}.
     *
     * @param name         the name of {@code QueryCache}
     * @param listener     the {@code MapListener} which will be used to listen this {@code QueryCache}
     * @param predicate    the predicate for filtering entries.
     * @param includeValue {@code true} if this {@code QueryCache} is allowed to cache values of entries, otherwise {@code false}
     * @return the {@code QueryCache} instance with the supplied {@code name}
     * @throws NullPointerException if the specified {@code name} or {@code listener} or {@code predicate} is null.
     * @see QueryCache
     * @since 3.5
     */
    QueryCache<K, V> getQueryCache(String name, MapListener listener, Predicate<K, V> predicate, boolean includeValue);
}

