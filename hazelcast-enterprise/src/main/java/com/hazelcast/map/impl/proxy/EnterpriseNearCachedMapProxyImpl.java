/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.proxy;

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
 * A server-side {@code IEnterpriseMap} implementation which is fronted by a near-cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class EnterpriseNearCachedMapProxyImpl<K, V> extends NearCachedMapProxyImpl<K, V> implements IEnterpriseMap<K, V> {

    public EnterpriseNearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
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

    protected QueryCache getQueryCacheInternal(String name, MapListener listener, Predicate predicate,
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
        EnterpriseMapServiceContext mapServiceContext
                = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }

    private QueryCache createQueryCache(QueryCacheRequest request) {
        ConstructorFunction<String, InternalQueryCache> constructorFunction
                = new NodeQueryCacheEndToEndConstructor(request);
        QueryCacheContext queryCacheContext = request.getContext();
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider queryCacheEndToEndProvider = subscriberContext.getEndToEndQueryCacheProvider();
        return queryCacheEndToEndProvider.getOrCreateQueryCache(request.getMapName(),
                request.getUserGivenCacheName(), constructorFunction);
    }
}
