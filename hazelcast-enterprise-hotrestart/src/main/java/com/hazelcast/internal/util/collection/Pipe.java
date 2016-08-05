package com.hazelcast.internal.util.collection;

import java.util.Collection;
import com.hazelcast.util.function.Consumer;

/**
 * A container for items processed in sequence.
 *
 * @param <E> type of elements in the pipe.
 */
public interface Pipe<E> {
    /**
     * The number of items added to this container since creation.
     *
     * @return the number of items added.
     */
    long addedCount();

    /**
     * The number of items removed from this container since creation.
     *
     * @return the number of items removed.
     */
    long removedCount();

    /**
     * The maximum capacity of this container to hold items.
     *
     * @return the capacity of the container.
     */
    int capacity();

    /**
     * Get the remaining capacity for elements in the container given the current size.
     *
     * @return remaining capacity of the container
     */
    int remainingCapacity();

    /**
     * Invoke a {@link Consumer} callback on each elements to drain the collection of elements until it is empty.
     *
     * If possible, implementations should use smart batching to best handle burst traffic.
     *
     * @param elementHandler to callback for processing elements
     * @return the number of elements drained
     */
    int drain(Consumer<E> elementHandler);

    /**
     * Drain available elements into the provided {@link java.util.Collection} up to a provided maximum limit of elements.
     *
     * If possible, implementations should use smart batching to best handle burst traffic.
     *
     * @param target in to which elements are drained.
     * @param limit  of the maximum number of elements to drain.
     * @return the number of elements actually drained.
     */
    int drainTo(Collection<? super E> target, int limit);
}
