package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.util.collection.QueuedPipe;

/**
 * Specialization of {@link ConcurrentConveyor} to a single queue.
 */
@SuppressWarnings("checkstyle:interfaceistype")
public final class ConcurrentConveyorSingleQueue<E> extends ConcurrentConveyor<E> {
    private final QueuedPipe<E> queue;

    private ConcurrentConveyorSingleQueue(E submitterGoneItem, QueuedPipe<E> queue) {
        super(submitterGoneItem, queue);
        this.queue = queue;
    }

    /**
     * Creates a new concurrent conveyor with a single queue.
     * @param submitterGoneItem the object that a submitter thread can use to signal it's done submitting
     * @param queue the concurrent queue the conveyor will manage
     */
    public static <E1> ConcurrentConveyorSingleQueue<E1> concurrentConveyorSingleQueue(
            E1 submitterGoneItem, QueuedPipe<E1> queue
    ) {
        return new ConcurrentConveyorSingleQueue<E1>(submitterGoneItem, queue);
    }

    /**
     * Offers an item to the queue.
     * @return whether the item was accepted by the queue
     * @throws ConcurrentConveyorException if the draining thread has already left
     */
    public boolean offer(E item) throws ConcurrentConveyorException {
        return offer(queue, item);
    }

    /**
     * Submits an item to the queue.
     * @throws ConcurrentConveyorException if the current thread is interrupted or the draining thread has already left
     */
    public void submit(E item) throws ConcurrentConveyorException {
        submit(queue, item);
    }
}
