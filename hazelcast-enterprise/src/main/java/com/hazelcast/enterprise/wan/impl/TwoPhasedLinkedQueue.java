package com.hazelcast.enterprise.wan.impl;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static java.util.Objects.requireNonNull;

/**
 * A non-intrusive, linked, unbounded MPMC queue with optional two-phased
 * dequeuing. The first phase is the usual consumption phase just like with
 * general-purpose queues. In this phase, the consumer dequeues the previously
 * enqueued values and can process them as usual with the guarantee that the next
 * consumptions will not ever see the same values. The consumers can choose from
 * the single-phased and the two-phased APIs. The single-phased API {@link #poll()}
 * is the API familiar from the general-purpose queues. The two-phased API
 * ({@link #dequeue()} retrieves the dequeued entry that has to be finalized through
 * the {@link Finalizer} this queue sets on it. Until the finalization is
 * performed, this non-finalized entry is considered to be part of the queue along
 * with the other already consumed entries (finalized or not) that follow the given
 * non-finalized entry. This means finalization doesn't necessarily means removal,
 * the finalized entries may stay in the queue as long as they are preceded by a
 * not yet finalized entry. This is useful for applications where a sequence of
 * entries need to be kept in their enqueue order for an optional retry. In this
 * case, the entries can be retrieved from the queue in the order they were enqueued
 * by calling the {@link #consumeAll(Consumer)} or {@link #consumeAll(Consumer, Consumer)}
 * without removing from the queue.
 * <p>
 * <p/>
 * <h4>DESIGN CHOICES</h4>
 * The queue was designed primarily to serve as the backing queue for WAN
 * replication partition queues, and this intent is reflected in the design choices
 * made both in terms of the the internal structure and the API of this queue. WAN
 * uses a single producer and a single consumer for each WAN queue, with the exception
 * of clearing the queue, which is performed by calling {@link #clear()} on this queue.
 * <p/>
 * Since the queue is effectively used in an SPSC environment with the aforementioned
 * {@link #clear()} exception, internally it uses locks that are not contended during
 * the typical case (no {@link #clear()}is invoked), therefore, the JVM optimize by
 * using not inflated locks, which is faster than any Java-level alternative (like
 * a CAS loop etc). The same technique is used by {@link LinkedBlockingQueue}, which
 * this queue meant to replace. There is no change in this matter.
 * <p>
 * <p/>
 * The queue intends to keep the producers and the consumers fast, and allows deferring
 * the finalization phase to an asynchronous thread. For example, when doing asynchronous
 * messaging, the asynchronous callback handler can finalize the dequeued entry.
 * <p>
 * <p/>
 * Since the queue has a custom API, it doesn't implement {@link java.util.Queue}.
 * <p>
 * <p/>
 * <h4>INTERNAL STRUCTURE</h4>
 * <p>
 * The queue operates with three pointers. All pointers are nodes in the queue and points
 * to the referenced entry through {@link Node#next}. This means for example that the next
 * entry to consume from the queue is accessible as {@code head.next}. The three pointers
 * are the following
 *
 * <ul>
 *     <li>(F)inalization head: points to the first node awaiting for finalization.
 *     <li>(H)ead: points to the first node awaiting for consumption.
 *     <li>(T)ail: points to the last node of the queue, the producer is to attach to
 *     this node.
 * </ul>
 * <p>
 * The below figure illustrates this structure. The queue in this figure has 3 nodes
 * waiting for finalization (labelled with 2,3,4) and 3 nodes waiting for consumption
 * (labeled with 5,6,7).
 *
 * <pre>
 * 7   6   5   4   3   2   1
 * O<--O<--O<--O<--O<--O<--O
 * ^           ^           ^
 * T           H           F
 * </pre>
 * <p>
 * Each of the pointer nodes are associated with a lock: the finalization head lock, the
 * head lock and the tail lock, respectively.
 *
 * @param <E> The type of the values the queue hold
 */
public class TwoPhasedLinkedQueue<E extends FinalizerAware> {
    private Node<E> head;
    private Node<E> tail;
    private Node<E> finalizationHead;
    private final Object headLock = new Object();
    private final Object tailLock = new Object();
    private final Object finalizationHeadLock = new Object();
    private final AtomicInteger size = new AtomicInteger();

    TwoPhasedLinkedQueue() {
        Node<E> initNode = new Node<>(null);
        finalizationHead = initNode;
        head = initNode;
        tail = initNode;
    }

    /**
     * Enqueues a value to the queue.
     *
     * @param value The value to enqueue
     * @return {@code true} if the value was successfully enqueued,
     * {@code false otherwise}
     * @throws NullPointerException if the provided value is {@code null}
     */
    public boolean offer(E value) {
        synchronized (tailLock) {
            Node<E> newTail = new Node<>(requireNonNull(value));
            Node<E> oldTail = tail;

            tail = newTail;
            oldTail.next = newTail;

            size.getAndIncrement();
        }

        return true;
    }

    /**
     * Returns and removes the head of the queue. The returned value
     * doesn't need finalization.
     *
     * @return the value taken from the head of the queue
     */
    public E poll() {
        return dequeueInternalSinglePhased();
    }

    /**
     * Returns the head of the queue. The returned value needs finalization.
     *
     * @return the dequeued the value
     */
    public E dequeue() {
        return dequeueInternalTwoPhased();
    }

    private E dequeueInternalSinglePhased() {
        Node<E> dequeued = dequeueInternal();
        if (dequeued == null) {
            return null;
        }

        finalizeNode(dequeued);

        return dequeued.value;
    }

    private E dequeueInternalTwoPhased() {
        Node<E> dequeued = dequeueInternal();
        if (dequeued == null) {
            return null;
        }

        E dequeuedValue = dequeued.value;
        dequeuedValue.setFinalizer(() -> finalizeNode(dequeued));
        return dequeuedValue;
    }

    private Node<E> dequeueInternal() {
        synchronized (headLock) {
            Node<E> dequeued = head.next;
            if (dequeued == null) {
                return null;
            }

            head = dequeued;
            size.getAndDecrement();
            dequeued.state = NodeState.CONSUMED;

            return dequeued;
        }
    }

    private void finalizeNode(Node<E> finalizedNode) {
        synchronized (finalizationHeadLock) {
            finalizedNode.state = NodeState.FINALIZED;

            Node<E> node = finalizationHead.next;

            while (node != null && node.state == NodeState.FINALIZED) {
                finalizationHead = node;
                node = node.next;
            }
        }
    }

    /**
     * Drains the requested number of entries from the head of the queue to the
     * provided {@code drainTo} collection. If the size of the queue is less than
     * the requested number of entries, the method successfully returns. The
     * entries drained will require finalization to get removed from the queue.
     *
     * @param drainTo         The collection the drained entries to put into
     * @param elementsToDrain The number of entries to drain
     * @return the number of the entries drained
     * @throws NullPointerException     if the provided {@code drainTo}
     *                                  collection is {@code null}
     * @throws IllegalArgumentException if the provided {@code elementsToDrain}
     *                                  parameter is negative
     */
    public int drainTo(Collection<E> drainTo, int elementsToDrain) {
        requireNonNull(drainTo);
        checkNotNegative(elementsToDrain, "The argument elementsToDrain must not be negative");

        boolean dequeued = true;
        int drained = 0;
        for (int i = 0; i < elementsToDrain && dequeued; i++) {
            E dequeuedValue = dequeueInternalTwoPhased();
            dequeued = dequeuedValue != null;
            if (dequeued) {
                drainTo.add(dequeuedValue);
                drained++;
            }
        }

        return drained;
    }

    /**
     * Clears the queue and returns the number of the values the queue
     * contained before the operation.
     *
     * @return the number of the values the queue contained before the operation
     */
    public int clear() {
        synchronized (tailLock) {
            synchronized (headLock) {
                synchronized (finalizationHeadLock) {
                    Node<E> newNode = new Node<>(null);
                    tail = newNode;
                    Node<E> oldHead = head;
                    head = newNode;
                    finalizationHead = newNode;

                    int countCleared = 0;
                    Node<E> node = oldHead;
                    while (node != null) {
                        if (node != oldHead) {
                            countCleared++;
                        }
                        node = node.next;
                    }
                    size.getAndAdd(-countCleared);

                    return countCleared;
                }
            }
        }
    }

    /**
     * Consumes all values from the queue with a single consumer without removing
     * them from the queue.
     *
     * @param consumer The consumer called for every value in the queue
     * @throws NullPointerException if the provided {@code consumer} is {@code null}
     */
    void consumeAll(Consumer<E> consumer) {
        requireNonNull(consumer);

        synchronized (headLock) {
            synchronized (finalizationHeadLock) {
                Node<E> fNode = finalizationHead.next;
                while (fNode != null && fNode.state != NodeState.OFFERED) {
                    consumer.accept(fNode.value);
                    fNode = fNode.next;
                }

                Node<E> qNode = head.next;
                while (qNode != null) {
                    consumer.accept(qNode.value);
                    qNode = qNode.next;
                }
            }
        }
    }

    /**
     * Consumes all values from the queue with two consumers without removing
     * them from the queue. One consumer is used for the entries waiting for
     * finalization, the second is for the values waiting for consumption.
     *
     * @param queueConsumer        The consumer called for every value waiting
     *                             for consumption
     * @param finalizationConsumer The consumer called for every value waiting
     *                             for finalization
     * @throws NullPointerException if any of the the provided consumers is {@code null}
     */
    void consumeAll(Consumer<E> queueConsumer, Consumer<E> finalizationConsumer) {
        requireNonNull(queueConsumer);
        requireNonNull(finalizationConsumer);

        synchronized (headLock) {
            synchronized (finalizationHeadLock) {
                Node<E> fNode = finalizationHead;
                while ((fNode = fNode.next) != null && fNode.state != NodeState.OFFERED) {
                    finalizationConsumer.accept(fNode.value);
                }

                Node<E> qNode = head;
                while ((qNode = qNode.next) != null) {
                    queueConsumer.accept(qNode.value);
                }
            }
        }
    }

    /**
     * Returns the number of entries waiting for consumption. The returned
     * size does not contain the number of the entries waiting for finalization.
     *
     * @return the size of the queue
     */

    public int size() {
        return size.get();
    }

    private static class Node<E> {
        private Node<E> next;
        private final E value;
        private NodeState state = NodeState.OFFERED;

        Node(E value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Node{"
                    + "value=" + value
                    + ", state=" + state.name()
                    + '}';
        }
    }

    private enum NodeState {
        OFFERED,
        CONSUMED,
        FINALIZED
    }
}
