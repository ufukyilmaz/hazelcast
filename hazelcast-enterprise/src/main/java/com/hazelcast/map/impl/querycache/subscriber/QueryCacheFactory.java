package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Static factory for simple {@link com.hazelcast.map.QueryCache QueryCache} implementations.
 */
class QueryCacheFactory {

    /**
     * Constructor for an instance of {@link com.hazelcast.map.QueryCache QueryCache}.
     */
    private static class InternalQueryCacheConstructor implements ConstructorFunction<String, InternalQueryCache> {

        private final QueryCacheRequest request;

        public InternalQueryCacheConstructor(QueryCacheRequest request) {
            this.request = request;
        }

        @Override
        public InternalQueryCache createNew(String ignored) {
            String cacheName = request.getCacheName();
            String userGivenCacheName = request.getUserGivenCacheName();
            IMap delegate = request.getMap();
            QueryCacheContext context = request.getContext();

            return new DefaultQueryCache(cacheName, userGivenCacheName, delegate, context);
        }
    }

    private final ConcurrentMap<String, InternalQueryCache> internalQueryCaches;

    public QueryCacheFactory() {
        this.internalQueryCaches = new ConcurrentHashMap<String, InternalQueryCache>();
    }

    public InternalQueryCache create(QueryCacheRequest request) {
        return ConcurrencyUtil.getOrPutIfAbsent(internalQueryCaches,
                request.getCacheName(), new InternalQueryCacheConstructor(request));
    }

    public InternalQueryCache getOrNull(String cacheName) {
        return internalQueryCaches.get(cacheName);
    }
}
