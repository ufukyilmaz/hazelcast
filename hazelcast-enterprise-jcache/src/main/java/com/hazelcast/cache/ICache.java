package com.hazelcast.cache;


import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.annotation.Beta;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Beta
public interface ICache<K, V> extends DistributedObject, javax.cache.Cache<K, V>  {

    /**
     * Asynchronously gets an entry from the cache.
     * <pre><code>
     * Future future = cache.getAsync(key);
     * // do some other stuff, when ready get the result
     * Object value = future.get();
     * </code></pre>
     * Future.get() will block until the actual cache.get() completes.
     * If the application requires timely response,
     * then Future.get(timeout, timeunit) can be used.
     * <pre><code>
     * try{
     * Future future = map.getAsync(key);
     * Object value = future.get(40, TimeUnit.MILLISECOND);
     * }catch (TimeoutException t) {
     * // time wasn't enough
     * }
     * </code></pre>
     *
     * @param key the key of the cache entry
     * @return Future from which the value of the key can be retrieved.
     * @throws NullPointerException if the specified key is null
     * @see java.util.concurrent.Future
     */
    Future<V> getAsync(K key);

    /**
     * Asynchronously associates the specified value with the specified key in the cache.
     * <pre><code>
     * Future future = cache.putAsync(key, value);
     * // do some other stuff, when ready get the result
     * Object oldValue = future.get();
     * </code></pre>
     * Future.get() will block until the actual cache.put() completes.
     * If the application requires timely response,
     * then Future.get(timeout, timeunit) can be used.
     * </pre><code>
     * try{
     * Future future = map.putAsync(key, newValue);
     * Object oldValue = future.get(40, TimeUnit.MILLISECOND);
     * }catch (TimeoutException t) {
     * // time wasn't enough
     * }
     * </code></pre>
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     * @return Future to be able to wait for operation to complete
     * @throws NullPointerException if the specified key or value is null
     * @see java.util.concurrent.Future
     */
    Future<Void> putAsync(K key, V value);

    /**
     * Associates the specified value with the specified key in the cache
     * using an explicit TTL.
     *
     * <p>For details see {@link #put(Object, Object)}</p>
     *
     * @param key  the key of the map entry
     * @param value  the new value of the map entry
     * @param ttl TTL of the map entry
     * @param timeUnit unit of the TTL value
     */
    void put(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * Asynchronously associates the specified value with the specified key in the cache
     * using an explicit TTL.
     *
     * <p>For details see {@link #put(Object, Object, long, java.util.concurrent.TimeUnit)}</p>
     *
     * @param key   the key of the map entry
     * @param value  the new value of the map entry
     * @param ttl TTL of the map entry
     * @param timeUnit unit of the TTL value
     * @return Future to be able to wait for operation to complete
     */
    Future<Void> putAsync(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     *
     * <p>For details see {@link #getAndPut(Object, Object)}</p>
     *
     * @param key  the key of the map entry
     * @param value  the new value of the map entry
     * @return Future from which the old value of the key can be retrieved.
     */
    Future<V> getAndPutAsync(K key, V value);

    /**
     * Associates the specified value with the specified key in this cache,
     * returning an existing value if one existed
     * using an explicit TTL.
     *
     * <p>For details see {@link #getAndPut(Object, Object)}</p>
     *
     * @param key  the key of the map entry
     * @param value the new value of the map entry
     * @param ttl TTL of the map entry
     * @param timeUnit unit of the TTL value
     * @return the value associated with the key at the start of the operation or
     *         null if none was associated
     */
    V getAndPut(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed
     * using an explicit TTL.
     *
     * <p>For details see {@link #getAndPut(Object, Object)}</p>
     *
     * @param key the key of the map entry
     * @param value  the new value of the map entry
     * @param ttl TTL of the map entry
     * @param timeUnit unit of the TTL value
     * @return Future from which the old value of the key can be retrieved.
     */
    Future<V> getAndPutAsync(K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * Asynchronously removes the mapping for a key from this cache if it is present.
     *
     * @param key the key of the map entry
     * @return Future from result of the operation can be retrieved.
     */
    Future<Boolean> removeAsync(K key);

    /**
     * Asynchronously removes the entry for a key.
     *
     * @param key  the key of the map entry
     * @return Future from which the old value of the key can be retrieved.
     */
    Future<V> getAndRemoveAsync(K key);

    /**
     * Returns the number of key-value mappings in this cache.
     * @return size of the cache
     */
    int size();

    /**
     * {@inheritDoc}
     *
     * <p>
     *     Fetches key-value mappings using default batch size (100).
     *     To specify a custom batch size use {@link #iterator(int)}.
     * </p>
     *
     */
    Iterator<Cache.Entry<K, V>> iterator();

    /**
     * Returns an iterator over this cache.
     *
     * @param fetchCount fetch count to retrieve entries in batches.
     * @return iterator
     */
    Iterator<Cache.Entry<K, V>> iterator(int fetchCount);

    /**
     * Returns statistics for this cache.
     * @return statistics
     */
    CacheStats getStats();

}
