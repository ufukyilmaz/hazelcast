package com.hazelcast.client.impl;

import com.hazelcast.client.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.client.impl.querycache.subscriber.ClientQueryCacheEndToEndConstructor;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.InternalQueryCache;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequests.newQueryCacheRequest;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains enterprise-part extensions to {@code ClientMapProxy}.
 */
public class EnterpriseClientMapProxyImpl extends ClientMapProxy implements IEnterpriseMap {

    /**
     * Holds {@link QueryCacheContext} for this proxy.
     * There should be only one {@link QueryCacheContext} instance exist.
     */
    private ConcurrentMap<String, QueryCacheContext> queryCacheContextHolder
            = new ConcurrentHashMap<String, QueryCacheContext>(1);

    private final ConstructorFunction<String, QueryCacheContext> queryCacheContextConstructorFunction
            = new ConstructorFunction<String, QueryCacheContext>() {
        @Override
        public QueryCacheContext createNew(String arg) {
            return new ClientQueryCacheContext(getContext());
        }
    };

    public EnterpriseClientMapProxyImpl(String serviceName, String name) {
        super(serviceName, name);

    }

    public QueryCacheContext getQueryContext() {
        return getOrPutIfAbsent(queryCacheContextHolder,
                "QueryCacheContext", queryCacheContextConstructorFunction);
    }

    @Override
    public QueryCache getQueryCache(String name) {
        checkNotNull(name, "name cannot be null");

        return getQueryCacheInternal(name, null, null, null, this);
    }

    @Override
    public QueryCache getQueryCache(String name, Predicate predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, null, predicate, includeValue, this);
    }

    @Override
    public QueryCache getQueryCache(String name, MapListener mapListener, Predicate predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        return getQueryCacheInternal(name, mapListener, predicate, includeValue, this);
    }

    protected QueryCache getQueryCacheInternal(String name, MapListener listener, Predicate predicate,
                                               Boolean includeValue, IMap map) {
        QueryCacheContext context = getQueryContext();
        QueryCacheRequest request = newQueryCacheRequest()
                .withUserGivenCacheName(name)
                .withCacheName(UuidUtil.newUnsecureUuidString())
                .withListener(listener)
                .withPredicate(predicate)
                .withIncludeValue(includeValue)
                .forMap(map)
                .withContext(context);

        return createQueryCache(request);
    }

    private QueryCache createQueryCache(QueryCacheRequest request) {
        ConstructorFunction<String, InternalQueryCache> constructorFunction
                = new ClientQueryCacheEndToEndConstructor(request);
        SubscriberContext subscriberContext = getQueryContext().getSubscriberContext();
        QueryCacheEndToEndProvider queryCacheEndToEndProvider = subscriberContext.getEndToEndQueryCacheProvider();
        return queryCacheEndToEndProvider.getOrCreateQueryCache(request.getMapName(),
                request.getUserGivenCacheName(), constructorFunction);
    }

}
