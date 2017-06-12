package com.hazelcast.elastic.queue;

/**
 * Consumer for primitive {@code long}s.
 */
public interface LongConsumer {

    boolean consume(long value);
}
