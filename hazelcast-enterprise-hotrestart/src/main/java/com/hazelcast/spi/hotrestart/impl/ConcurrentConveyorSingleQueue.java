package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.util.concurrent.AbstractConcurrentArrayQueue;

/**
 * Specialization of {@link ConcurrentConveyor} to a single queue.
 */
@SuppressWarnings("checkstyle:interfaceistype")
public final class ConcurrentConveyorSingleQueue<E> extends ConcurrentConveyor<E> {
    private final AbstractConcurrentArrayQueue<E> queue;

    private ConcurrentConveyorSingleQueue(E submitterGoneItem, AbstractConcurrentArrayQueue<E> queue) {
        super(submitterGoneItem, queue);
        this.queue = queue;
    }

    public static <E1> ConcurrentConveyorSingleQueue<E1> concurrentConveyorSingleQueue(
            E1 submitterGoneItem, AbstractConcurrentArrayQueue<E1> queue
    ) {
        return new ConcurrentConveyorSingleQueue<E1>(submitterGoneItem, queue);
    }

    public boolean offer(E item) {
        return offer(queue, item);
    }

    public void submit(E item) {
        submit(queue, item);
    }
}
