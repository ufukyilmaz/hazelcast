package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.RamStoreRegistry;
import com.hazelcast.internal.hotrestart.impl.RestartItem.WithSetOfKeyHandle;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Contains the main loop executed by the threads that call into the {@code RamStore}
 * during the Hot Restart process.
 */
public class RamStoreRestartLoop {
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int QUEUE_CAPACITY_BUDGET = 1 << 12;
    public static final int MIN_QUEUE_CAPACITY = 8;
    /** How many times to busy-spin while awaiting new items in the queue. */
    public static final int SPIN_COUNT = 1;
    /** How many times to yield while awaiting new items in the queue. */
    public static final int YIELD_COUNT = 1;
    /** Max park microseconds while awaiting new items in the queue. */
    public static final long MAX_PARK_MICROS = 10;
    public static final IdleStrategy DRAIN_IDLER =
            new BackoffIdleStrategy(SPIN_COUNT, YIELD_COUNT, 1, MICROSECONDS.toNanos(MAX_PARK_MICROS));

    public final ConcurrentConveyorSingleQueue<RestartItem>[][] keyReceivers;
    public final ConcurrentConveyor<RestartItem>[] keyHandleSenders;
    public final ConcurrentConveyorSingleQueue<RestartItem>[][] valueReceivers;

    private final RestartItem keySubmitterGone;
    private final RestartItem.WithSetOfKeyHandle valueSubmitterGone;
    private final RamStoreRegistry reg;
    private final ILogger logger;

    private boolean submitterGoneSent;

    public RamStoreRestartLoop(int storeCount, int threadCount, RamStoreRegistry reg, ILogger logger) {
        this.reg = reg;
        this.logger = logger;
        this.keyReceivers = makeSingleQueueConveyors(storeCount, threadCount);
        this.keyHandleSenders = makeMultiQueueConveyors(storeCount, threadCount);
        this.valueReceivers = makeSingleQueueConveyors(storeCount, threadCount);
        this.keySubmitterGone = keyReceivers[0][0].submitterGoneItem();
        this.valueSubmitterGone = (RestartItem.WithSetOfKeyHandle) valueReceivers[0][0].submitterGoneItem();
    }

    public final void run(int threadIndex) {
        final int storeCount = keyHandleSenders.length;
        final int remainder = threadIndex % storeCount;
        final int quotient = threadIndex / storeCount;
        final ConcurrentConveyor<RestartItem> keyReceiver = keyReceivers[remainder][quotient];
        final ConcurrentConveyor<RestartItem> keyHandleSender = keyHandleSenders[remainder];
        final QueuedPipe<RestartItem> keyHandleSendQ = keyHandleSender.queue(quotient);
        final ConcurrentConveyor<RestartItem> valueReceiver = valueReceivers[remainder][quotient];
        try {
            keyReceiver.drainerArrived();
            valueReceiver.drainerArrived();
            mainLoop(threadIndex, keyReceiver, keyHandleSender, keyHandleSendQ, valueReceiver);
            keyReceiver.drainerDone();
            valueReceiver.drainerDone();
        } catch (Throwable t) {
            keyReceiver.drainerFailed(t);
            valueReceiver.drainerFailed(t);
            sneakyThrow(t);
        } finally {
            ensureSubmitterGoneSent(keyHandleSender, keyHandleSendQ);
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void mainLoop(
            int threadIndex, ConcurrentConveyor<RestartItem> keyReceiver,
            ConcurrentConveyor<RestartItem> keyHandleSender, QueuedPipe<RestartItem> sendQ,
            ConcurrentConveyor<RestartItem> valueReceiver
    ) {
        final int capacity = keyReceiver.queue(0).capacity();
        final Deque<RestartItem> keyBatch = new ArrayDeque<RestartItem>(capacity);
        final List<RestartItem> valueBatch = new ArrayList<RestartItem>(capacity);
        final RestartItem keyHandleSubmitterGoneItem = keyHandleSender.submitterGoneItem();
        RestartItem danglingDownstreamItem = null;
        long idleCount = 0;
        boolean didWork = true;
        long keyCount = 0;
        long keyDrainCount = 0;
        mainLoop:
        while (true) {
            if (currentThread().isInterrupted()) {
                throw new HotRestartException("Thread interrupted inside RamStoreRestartLoop");
            }
            if (didWork) {
                idleCount = 0;
            } else {
                DRAIN_IDLER.idle(idleCount++);
            }
            didWork = false;
            valueBatch.clear();
            valueReceiver.drainTo(valueBatch);
            for (RestartItem item : valueBatch) {
                if (consumeValueItem(item)) {
                    break mainLoop;
                }
                didWork = true;
            }
            final int count = keyReceiver.drainTo(keyBatch, capacity - keyBatch.size());
            // Maintain counters used to determine the mean queue size over the whole run:
            if (count > 0) {
                keyCount += count;
                keyDrainCount++;
            }
            if (danglingDownstreamItem != null) {
                if (keyHandleSender.offer(sendQ, danglingDownstreamItem)) {
                    danglingDownstreamItem = null;
                } else {
                    // Don't drain more key items until the dangling keyHandle item is sent
                    continue;
                }
            }
            for (RestartItem item; (item = keyBatch.pollFirst()) != null; ) {
                final RestartItem downstreamItem = consumeKeyItem(item, keyHandleSubmitterGoneItem);
                didWork = true;
                // Offer to keyHandleSender instead of submitting. If we block trying to submit,
                // we risk a deadlock where the downstream thread is blocking to submit back
                // to the value queue, thus unable to drain our keyHandle queue; and we are
                // not draining the value queue while blocking here.
                if (!keyHandleSender.offer(sendQ, downstreamItem)) {
                    danglingDownstreamItem = downstreamItem;
                    break;
                }
            }
        }
        logger.fine(String.format("threadIndex %d: drained %,d items, mean queue size was %.1f (capacity %,d)",
                threadIndex, keyCount, (double) keyCount / keyDrainCount, capacity));
    }

    private RestartItem consumeKeyItem(RestartItem item, RestartItem keyHandleSubmitterGoneItem) {
        if (!item.isSpecialItem()) {
            item.ramStore = reg.restartingRamStoreForPrefix(item.prefix);
            item.keyHandle = item.ramStore.toKeyHandle(item.key);
            return item;
        }
        if (item.isClearedItem()) {
            return item;
        }
        assert item == keySubmitterGone;
        submitterGoneSent = true;
        return keyHandleSubmitterGoneItem;
    }

    private boolean consumeValueItem(RestartItem item) {
        if (!item.isSpecialItem()) {
            item.ramStore.accept(item.keyHandle, item.value);
            return false;
        }
        if (item.isClearedItem()) {
            return false;
        }
        if (item == valueSubmitterGone) {
            return true;
        }
        final SetOfKeyHandle sokh = ((WithSetOfKeyHandle) item).sokh;
        reg.ramStoreForPrefix(item.prefix).removeNullEntries(sokh);
        return false;
    }

    private static ConcurrentConveyor<RestartItem>[] makeMultiQueueConveyors(
            int storeCount, int threadCount
    ) {
        final ConcurrentConveyor<RestartItem>[] conveyors = new ConcurrentConveyor[storeCount];
        final QueueParams qp = new QueueParams(storeCount, threadCount);
        for (int storeIndex = 0; storeIndex < storeCount; storeIndex++) {
            conveyors[storeIndex] = concurrentConveyor(RestartItem.END, qp.makeQueues(storeIndex));
        }
        return conveyors;
    }


    private static ConcurrentConveyorSingleQueue<RestartItem>[][] makeSingleQueueConveyors(
            int storeCount, int threadCount
    ) {
        final ConcurrentConveyorSingleQueue<RestartItem>[][] conveyorArrays =
                new ConcurrentConveyorSingleQueue[storeCount][];
        final QueueParams qp = new QueueParams(storeCount, threadCount);
        for (int storeIndex = 0; storeIndex < conveyorArrays.length; storeIndex++) {
            final int submitterCount = qp.submitterCount(storeIndex);
            final ConcurrentConveyorSingleQueue<RestartItem>[] conveyors =
                    new ConcurrentConveyorSingleQueue[submitterCount];
            for (int submitterIndex = 0; submitterIndex < submitterCount; submitterIndex++) {
                conveyors[submitterIndex] = concurrentConveyorSingleQueue(RestartItem.END,
                        new OneToOneConcurrentArrayQueue<RestartItem>(qp.queueCapacity));
            }
            conveyorArrays[storeIndex] = conveyors;
        }
        return conveyorArrays;
    }

    @SuppressWarnings("checkstyle:emptyblock")
    private void ensureSubmitterGoneSent(ConcurrentConveyor<RestartItem> conveyor, Queue<RestartItem> queue) {
        if (!submitterGoneSent) {
            try {
                conveyor.submit(queue, conveyor.submitterGoneItem());
            } catch (Exception ignored) { }
        }
    }

    private static class QueueParams {
        final int queueCapacity;
        private final int quotient;
        private final int remainder;

        QueueParams(int storeCount, int threadCount) {
            this.queueCapacity = max(QUEUE_CAPACITY_BUDGET / (threadCount / storeCount), MIN_QUEUE_CAPACITY);
            final int maxThreadIndex = threadCount - 1;
            this.quotient = maxThreadIndex / storeCount;
            this.remainder = maxThreadIndex % storeCount;
        }

        /** Number of partition threads associated with the HR store at storeIndex */
        int submitterCount(int storeIndex) {
            return quotient + (storeIndex <= remainder ? 1 : 0);
        }

        OneToOneConcurrentArrayQueue<RestartItem>[] makeQueues(int storeIndex) {
            final OneToOneConcurrentArrayQueue<RestartItem>[] qs =
                    new OneToOneConcurrentArrayQueue[submitterCount(storeIndex)];
            for (int i = 0; i < qs.length; i++) {
                qs[i] = new OneToOneConcurrentArrayQueue<RestartItem>(queueCapacity);
            }
            return qs;
        }
    }

}
