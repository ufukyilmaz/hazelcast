package com.hazelcast.internal.util.collection;

import java.util.Queue;

/**
 * Composed interface for concurrent queues and sequenced containers.
 *
 * @param <E> type of the elements stored in the {@link java.util.Queue}.
 */
public interface QueuedPipe<E> extends Queue<E>, Pipe<E> {
}
