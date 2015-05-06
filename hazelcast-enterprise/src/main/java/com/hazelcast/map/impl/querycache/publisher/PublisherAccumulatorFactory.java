package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorFactory;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;

/**
 * Factory which is responsible for creating {@link Accumulator} implementations.
 * according to supplied {@link AccumulatorInfo}.
 *
 * @see Accumulator
 */
public class PublisherAccumulatorFactory implements AccumulatorFactory {

    private final QueryCacheContext context;

    public PublisherAccumulatorFactory(QueryCacheContext context) {
        this.context = context;
    }

    @Override
    public Accumulator createAccumulator(AccumulatorInfo info) {
        long delayTime = info.getDelaySeconds();

        if (delayTime <= 0L) {
            return new NonStopPublisherAccumulator(context, info);
        } else {
            if (info.isCoalesce()) {
                return new CoalescingPublisherAccumulator(context, info);
            } else {
                return new BatchPublisherAccumulator(context, info);
            }
        }
    }

}

