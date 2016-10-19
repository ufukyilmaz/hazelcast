package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.NodeQueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;

import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequests.newQueryCacheRequest;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A server-side {@code IEnterpriseMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class EnterpriseNearCachedMapProxyImpl<K, V> extends NearCachedMapProxyImpl<K, V> implements IEnterpriseMap<K, V> {

    public EnterpriseNearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name) {
        checkNotNull(name, "name cannot be null");

        return getQueryCacheInternal(name, null, null, null, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, null, predicate, includeValue, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, listener, predicate, includeValue, this);
    }

    private QueryCache<K, V> getQueryCacheInternal(String name, MapListener listener, Predicate predicate,
                                                   Boolean includeValue, IMap map) {
        QueryCacheContext queryCacheContext = getQueryCacheContext();

        QueryCacheRequest request = newQueryCacheRequest()
                .forMap(map)
                .withCacheName(UuidUtil.newUnsecureUuidString())
                .withUserGivenCacheName(name)
                .withListener(listener)
                .withPredicate(predicate)
                .withIncludeValue(includeValue)
                .withContext(queryCacheContext);

        return createQueryCache(request);
    }

    private QueryCacheContext getQueryCacheContext() {
        MapService mapService = getService();
        EnterpriseMapServiceContext mapServiceContext = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }

    private QueryCache<K, V> createQueryCache(QueryCacheRequest request) {
        ConstructorFunction<String, InternalQueryCache> constructorFunction = new NodeQueryCacheEndToEndConstructor(request);
        QueryCacheContext queryCacheContext = request.getContext();
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider queryCacheEndToEndProvider = subscriberContext.getEndToEndQueryCacheProvider();
        return queryCacheEndToEndProvider.getOrCreateQueryCache(request.getMapName(),
                request.getUserGivenCacheName(), constructorFunction);
    }
}
