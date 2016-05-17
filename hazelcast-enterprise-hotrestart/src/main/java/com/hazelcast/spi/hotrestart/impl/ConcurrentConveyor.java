package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.util.concurrent.AbstractConcurrentArrayQueue;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.Collection;
import java.util.Queue;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * A many-to-one conveyor of interthread messages. Allows a setup where communication from N submitter threads
 * to 1 drainer thread happens over N one-to-one concurrent queues.
 * <p>
 * Allows the drainer thread to signal completion and failure to the submitters and make their blocking
 * {@code submit()} calls fail with an exception. This mechanism supports building an implementation which
 * is both starvation-safe and uses bounded queues with blocking queue submission.
 * <p>
 * There is a further option for the drainer to apply immediate backpressure to the submitter by invoking
 * {@link #backpressureOn()}. This will make the {@code submit()} invocations block after having
 * successfully submitted their item, until the drainer calls {@link #backpressureOff()} or fails.
 * This mechanism allows the drainer to apply backpressure and keep draining the queue, thus letting
 * all submitters progress until after submitting their item. Such an arrangement eliminates a class
 * of deadlock patterns where the submitter blocks to submit the item that would have made the drainer
 * remove backpressure.
 * <p>
 * Does not manage drainer threads. There should be only one drainer thread at a time.
 *
 */
@SuppressWarnings("checkstyle:interfaceistype")
public class ConcurrentConveyor<E> {
    /** How many times to busy-spin while waiting to submit to the work queue. */
    public static final int SUBMIT_SPIN_COUNT = 1000;
    /** How many times to yield while waiting to submit to the work queue. */
    public static final int SUBMIT_YIELD_COUNT = 200;
    /** Max park microseconds while waiting to submit to the work queue. */
    public static final long SUBMIT_MAX_PARK_MICROS = 200;
    /** Idling strategy suitable for the drainer thread's main loop. */
    public static final IdleStrategy SUBMIT_IDLER = new BackoffIdleStrategy(
            SUBMIT_SPIN_COUNT, SUBMIT_YIELD_COUNT, 1, MICROSECONDS.toNanos(SUBMIT_MAX_PARK_MICROS));

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private static final Throwable REGULAR_DEPARTURE = regularDeparture();
    private final AbstractConcurrentArrayQueue<E>[] queues;
    private final E submitterGoneItem;

    private volatile boolean backpressure;
    private volatile Thread drainer;
    private volatile Throwable drainerDepartureCause;

    ConcurrentConveyor(E submitterGoneItem, AbstractConcurrentArrayQueue<E>... queues) {
        if (queues.length == 0) {
            throw new IllegalArgumentException("No concurrent queues supplied");
        }
        this.submitterGoneItem = submitterGoneItem;
        this.queues = queues;
    }

    public static <E1> ConcurrentConveyor<E1> concurrentConveyor(
            E1 submitterGoneItem, AbstractConcurrentArrayQueue<E1>... queues
    ) {
        return new ConcurrentConveyor<E1>(submitterGoneItem, queues);
    }

    public final E submitterGoneItem() {
        return submitterGoneItem;
    }

    public final int queueCount() {
        return queues.length;
    }

    public final AbstractConcurrentArrayQueue<E> queue(int index) {
        return queues[index];
    }

    public final boolean offer(int queueIndex, E item) {
        return offer(queues[queueIndex], item);
    }

    public final boolean offer(Queue<E> queue, E item) {
        if (queue.offer(item)) {
            return true;
        } else {
            checkDrainerGone();
            unparkDrainer();
            return false;
        }
    }

    public final void submit(Queue<E> queue, E item) {
        for (long idleCount = 0; !queue.offer(item); idleCount++) {
            SUBMIT_IDLER.idle(idleCount);
            checkDrainerGone();
            unparkDrainer();
            checkInterrupted();
        }
        for (long idleCount = 0; backpressure; idleCount++) {
            SUBMIT_IDLER.idle(idleCount);
            checkInterrupted();
        }
    }

    public final int drainTo(Collection<? super E> drain) {
        return drain(queues[0], drain, Integer.MAX_VALUE);
    }

    public final int drainTo(int queueIndex, Collection<? super E> drain) {
        return drain(queues[queueIndex], drain, Integer.MAX_VALUE);
    }

    public final int drainTo(Collection<? super E> drain, int limit) {
        return drain(queues[0], drain, limit);
    }

    public final int drainTo(int queueIndex, Collection<? super E> drain, int limit) {
        return drain(queues[queueIndex], drain, limit);
    }

    public final void drainerArrived() {
        drainerDepartureCause = null;
        drainer = currentThread();
    }

    public final void drainerFailed(Throwable t) {
        drainer = null;
        drainerDepartureCause = t;
    }

    public final void drainerDone() {
        drainer = null;
        drainerDepartureCause = REGULAR_DEPARTURE;
    }

    public final boolean isDrainerGone() {
        return drainerDepartureCause != null;
    }

    public final void reset() {
        drainer = null;
        drainerDepartureCause = null;
        backpressure = false;
        for (AbstractConcurrentArrayQueue<E> queue : queues) {
            queue.clear();
        }
    }

    public final void backpressureOn() {
        backpressure = true;
    }

    public final void backpressureOff() {
        backpressure = false;
    }

    public final void awaitDrainerGone() {
        for (long i = 0; !isDrainerGone(); i++) {
            SUBMIT_IDLER.idle(i);
        }
        propagateDrainerFailure(drainerDepartureCause);
    }

    public final void checkDrainerGone() {
        final Throwable cause = drainerDepartureCause;
        if (cause == REGULAR_DEPARTURE) {
            throw new ConcurrentConveyorException("Queue drainer has already left");
        }
        propagateDrainerFailure(cause);
    }

    private int drain(AbstractConcurrentArrayQueue<E> q, Collection<? super E> drain, int limit) {
        return q.drainTo(drain, limit);
    }

    private void unparkDrainer() {
        final Thread drainer = this.drainer;
        if (drainer != null) {
            unpark(drainer);
        }
    }

    private void propagateDrainerFailure(Throwable cause) {
        if (cause != null && cause != REGULAR_DEPARTURE) {
            throw new ConcurrentConveyorException("Queue drainer failed", cause);
        }
    }

    private static void checkInterrupted() {
        if (currentThread().isInterrupted()) {
            throw new ConcurrentConveyorException("Thread interrupted");
        }
    }

    private static ConcurrentConveyorException regularDeparture() {
        final ConcurrentConveyorException e = new ConcurrentConveyorException("Regular departure");
        e.setStackTrace(new StackTraceElement[0]);
        return e;
    }
}
