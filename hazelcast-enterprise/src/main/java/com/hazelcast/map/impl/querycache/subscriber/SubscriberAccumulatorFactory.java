package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorFactory;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.QueryCacheContext;

/**
 * Subscriber part implementation of {@link AccumulatorFactory}
 *
 * @see AccumulatorFactory
 */
public class SubscriberAccumulatorFactory implements AccumulatorFactory {

    private final QueryCacheContext context;

    public SubscriberAccumulatorFactory(QueryCacheContext context) {
        this.context = context;
    }

    @Override
    public Accumulator createAccumulator(AccumulatorInfo info) {
        return new SubscriberAccumulator(context, info);
    }

    protected QueryCacheContext getContext() {
        return context;
    }
}
