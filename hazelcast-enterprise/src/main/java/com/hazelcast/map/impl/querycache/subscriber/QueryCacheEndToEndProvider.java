package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Provides construction of whole {@link com.hazelcast.map.QueryCache QueryCache}
 * sub-system. As a result of that construction we can have a ready to use {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public class QueryCacheEndToEndProvider {

    private static final int MUTEX_COUNT = 16;
    private static final int MASK = MUTEX_COUNT - 1;
    private final Object[] mutexes;
    private final ConcurrentMap<String, InternalQueryCache> queryCaches;

    public QueryCacheEndToEndProvider() {
        mutexes = createMutexes();
        queryCaches = new ConcurrentHashMap<String, InternalQueryCache>();
    }

    private Object[] createMutexes() {
        Object[] mutexes = new Object[MUTEX_COUNT];
        for (int i = 0; i < MUTEX_COUNT; i++) {
            mutexes[i] = new Object();
        }
        return mutexes;
    }

    private Object getMutex(String name) {
        int hashCode = name.hashCode();
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = 0;
        }
        hashCode = Math.abs(hashCode);
        return mutexes[hashCode & MASK];
    }

    public InternalQueryCache getOrCreateQueryCache(String name, ConstructorFunction<String, InternalQueryCache> constructor) {
        Object mutex = getMutex(name);
        synchronized (mutex) {
            InternalQueryCache cache = getOrPutSynchronized(queryCaches, name, mutex, constructor);
            if (cache == NULL_QUERY_CACHE) {
                remove(name);
                return null;
            }
            return cache;
        }
    }

    public InternalQueryCache remove(String cacheName) {
        return queryCaches.remove(cacheName);
    }

}
