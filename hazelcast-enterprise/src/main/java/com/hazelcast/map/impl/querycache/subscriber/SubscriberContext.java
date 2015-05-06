package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;

/**
 * Context contract for subscriber side of a {@link com.hazelcast.map.QueryCache QueryCache} implementation.
 */
public interface SubscriberContext {

    /**
     * Returns {@link QueryCacheEventService} for this context.
     *
     * @return {@link QueryCacheEventService} for this context.
     * @see QueryCacheEventService
     */
    QueryCacheEventService getEventService();

    /**
     * Returns {@link QueryCacheEndToEndProvider} for this context.
     *
     * @return {@link QueryCacheEndToEndProvider} for this context.
     * @see QueryCacheEndToEndProvider
     */
    QueryCacheEndToEndProvider getEndToEndQueryCacheProvider();

    /**
     * Returns {@link QueryCacheConfigurator} for this context.
     *
     * @return {@link QueryCacheConfigurator} for this context.
     * @see QueryCacheConfigurator
     */
    QueryCacheConfigurator geQueryCacheConfigurator();


    /**
     * Returns {@link QueryCacheFactory} for this context.
     *
     * @return {@link QueryCacheFactory} for this context.
     * @see QueryCacheFactory
     */
    QueryCacheFactory getQueryCacheFactory();

    /**
     * Returns {@link AccumulatorInfoSupplier} for this context.
     *
     * @return {@link AccumulatorInfoSupplier} for this context.
     * @see AccumulatorInfoSupplier
     */
    AccumulatorInfoSupplier getAccumulatorInfoSupplier();

    /**
     * Returns {@link MapSubscriberRegistry} for this context.
     *
     * @return {@link MapSubscriberRegistry} for this context.
     * @see MapSubscriberRegistry
     */
    MapSubscriberRegistry getMapSubscriberRegistry();

    /**
     * Returns {@link SubscriberContextSupport} for this context.
     *
     * @return {@link SubscriberContextSupport} for this context.
     * @see SubscriberContextSupport
     */
    SubscriberContextSupport getSubscriberContextSupport();
}
