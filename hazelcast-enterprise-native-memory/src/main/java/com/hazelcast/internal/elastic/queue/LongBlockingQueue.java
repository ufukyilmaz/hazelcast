package com.hazelcast.internal.elastic.queue;

import java.util.concurrent.TimeUnit;

/**
 * Blocking queue for primitive {@code long}s.
 */
public interface LongBlockingQueue extends LongQueue {

    boolean offer(long value, long timeout, TimeUnit unit) throws InterruptedException;

    void put(long value) throws InterruptedException;

    long poll(long timeout, TimeUnit unit) throws InterruptedException;

    long take() throws InterruptedException;

    void consume(LongConsumer consumer);
}
