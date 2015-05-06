package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.publisher.PublisherAccumulatorHandler;

/**
 * Used to handle elements of an {@link Accumulator}.
 *
 * @param <T> the type of element in {@link Accumulator}
 * @see PublisherAccumulatorHandler
 * @see com.hazelcast.map.impl.querycache.subscriber.SubscriberAccumulatorHandler
 */
public interface AccumulatorHandler<T> {

    /**
     * Handles element.
     *
     * @param element     the element to be processed.
     * @param lastElement {@code true} if this is the last element got from the {@code Accumulator}, otherwise {@code false}.
     */
    void handle(T element, boolean lastElement);
}
