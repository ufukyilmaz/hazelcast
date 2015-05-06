package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.util.ConstructorFunction;

/**
 * Constructor which is used to construct all parts/sides of a {@code QueryCache} system on nodes or on clients.
 * This constructor is called from subscriber-side and initiates the construction of whole underlying system
 * which feeds the {@code QueryCache}.
 * <p/>
 * When this constructor returns successfully, there should be a {@code QueryCache} instance ready to use.
 *
 * @see com.hazelcast.map.QueryCache
 */
public interface QueryCacheEndToEndConstructor extends ConstructorFunction<String, InternalQueryCache> {

    /**
     * Creates a new {@link SubscriberAccumulator}
     *
     * @param info {@link AccumulatorInfo} for the {@link SubscriberAccumulator}
     * @throws Exception in case of any exceptional case.
     */
    void createSubscriberAccumulator(AccumulatorInfo info) throws Exception;

    /**
     * Creates a new publisher-accumulator according to {@link AccumulatorInfo}.
     * This accumulator will be used to feed the {@link SubscriberAccumulator}.
     *
     * @param info {@link AccumulatorInfo} for the publisher-accumulator
     * @throws Exception in case of any exceptional case.
     * @see com.hazelcast.map.impl.querycache.publisher.NonStopPublisherAccumulator
     * @see com.hazelcast.map.impl.querycache.publisher.BatchPublisherAccumulator
     * @see com.hazelcast.map.impl.querycache.publisher.CoalescingPublisherAccumulator
     */
    void createPublisherAccumulator(AccumulatorInfo info) throws Exception;

}
