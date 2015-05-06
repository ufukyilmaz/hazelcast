package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.accumulator.DefaultAccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;

/**
 * Abstract implementation of {@code SubscriberContext}.
 * This class is used by both client and node parts.
 *
 * @see SubscriberContext
 */
public abstract class AbstractSubscriberContext implements SubscriberContext {

    private final QueryCacheEventService eventService;
    private final QueryCacheEndToEndProvider queryCacheEndToEndProvider;
    private final MapSubscriberRegistry mapSubscriberRegistry;
    private final QueryCacheConfigurator queryCacheConfigurator;
    private final QueryCacheFactory queryCacheFactory;
    private final DefaultAccumulatorInfoSupplier accumulatorInfoSupplier;

    public AbstractSubscriberContext(QueryCacheContext context) {
        this.queryCacheConfigurator = context.getQueryCacheConfigurator();
        this.eventService = context.getQueryCacheEventService();
        this.queryCacheEndToEndProvider = new QueryCacheEndToEndProvider();
        this.mapSubscriberRegistry = new MapSubscriberRegistry(context);
        this.queryCacheFactory = new QueryCacheFactory();
        this.accumulatorInfoSupplier = new DefaultAccumulatorInfoSupplier();
    }

    @Override
    public QueryCacheEndToEndProvider getEndToEndQueryCacheProvider() {
        return queryCacheEndToEndProvider;
    }

    @Override
    public MapSubscriberRegistry getMapSubscriberRegistry() {
        return mapSubscriberRegistry;
    }

    @Override
    public QueryCacheFactory getQueryCacheFactory() {
        return queryCacheFactory;
    }

    @Override
    public AccumulatorInfoSupplier getAccumulatorInfoSupplier() {
        return accumulatorInfoSupplier;
    }

    @Override
    public QueryCacheEventService getEventService() {
        return eventService;
    }

    @Override
    public QueryCacheConfigurator geQueryCacheConfigurator() {
        return queryCacheConfigurator;
    }


}
