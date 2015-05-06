package com.hazelcast.map.impl.querycache.accumulator;

/**
 * Factory which is used to create an {@link Accumulator}.
 *
 * @see com.hazelcast.map.impl.querycache.publisher.PublisherAccumulatorFactory
 * @see com.hazelcast.map.impl.querycache.subscriber.SubscriberAccumulatorFactory
 */
public interface AccumulatorFactory {

    /**
     * @param info info which will be used by an {@link Accumulator}
     * @return an instance of {@link Accumulator}
     */
    Accumulator createAccumulator(AccumulatorInfo info);
}
