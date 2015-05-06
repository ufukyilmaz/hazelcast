package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.BasicAccumulator;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

import java.util.concurrent.TimeUnit;

/**
 * When batching enabled by setting {@link com.hazelcast.config.QueryCacheConfig#batchSize} to a value greater than 1,
 * this {@code Accumulator} is used to collect events. This {@code Accumulator} uses batch publishing logic in
 * the {@link EventPublisherAccumulatorProcessor} class.
 *
 * @see EventPublisherAccumulatorProcessor
 */
class BatchPublisherAccumulator extends BasicAccumulator<Sequenced> {

    BatchPublisherAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        super(context, info);
    }

    @Override
    public void accumulate(Sequenced sequenced) {
        super.accumulate(sequenced);

        AccumulatorInfo info = getInfo();
        if (!info.isPublishable()) {
            return;
        }

        poll(handler, info.getBatchSize());
        poll(handler, info.getDelaySeconds(), TimeUnit.SECONDS);
    }

}
