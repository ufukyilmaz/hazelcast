package com.hazelcast.map.impl.querycache.accumulator;

/**
 * Responsible for processing of an event of an {@link Accumulator}.
 * <p/>
 * Processing can be vary according to the implementation.
 *
 * @param <T> type of element to process
 * @see com.hazelcast.map.impl.querycache.publisher.EventPublisherAccumulatorProcessor
 */
public interface AccumulatorProcessor<T> {

    /**
     * Processes event.
     *
     * @param event type of event.
     */
    void process(T event);

}
