package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.RestartItem.WithSetOfKeyHandle;
import com.hazelcast.util.concurrent.AbstractConcurrentArrayQueue;
import com.hazelcast.util.concurrent.OneToOneConcurrentArrayQueue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.ConcurrentConveyor.IDLER;
import static com.hazelcast.spi.hotrestart.impl.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.spi.hotrestart.impl.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;

/**
 * Contains the main loop executed by the threads that call into the {@code RamStore}
 * during the Hot Restart process.
 */
public class RamStoreRestartLoop {
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int QUEUE_CAPACITY_BUDGET = 1 << 12;
    public static final int MIN_QUEUE_CAPACITY = 8;

    public final ConcurrentConveyorSingleQueue<RestartItem>[] keyReceivers;
    public final ConcurrentConveyor<RestartItem> keyHandleSender;
    public final ConcurrentConveyorSingleQueue<RestartItem>[] valueReceivers;

    private final RestartItem keySubmitterGone;
    private final RestartItem.WithSetOfKeyHandle valueSubmitterGone;
    private final RamStoreRegistry reg;
    private final ILogger logger;

    private boolean submitterGoneSent;

    public RamStoreRestartLoop(RamStoreRegistry reg, int threadCount, ILogger logger) {
        this.reg = reg;
        this.logger = logger;
        this.keyReceivers = makeConveyors(threadCount);
        this.keyHandleSender = concurrentConveyor(RestartItem.END, makeQueues(threadCount));
        this.valueReceivers = makeConveyors(threadCount);
        this.keySubmitterGone = keyReceivers[0].submitterGoneItem();
        this.valueSubmitterGone = (RestartItem.WithSetOfKeyHandle) valueReceivers[0].submitterGoneItem();
    }

    public final void run(int queueIndex) {
        final ConcurrentConveyor<RestartItem> keyReceiver = keyReceivers[queueIndex];
        final ConcurrentConveyor<RestartItem> valueReceiver = valueReceivers[queueIndex];
        final AbstractConcurrentArrayQueue<RestartItem> sendQ = keyHandleSender.queue(queueIndex);
        try {
            keyReceiver.drainerArrived();
            valueReceiver.drainerArrived();
            mainLoop(keyReceiver, valueReceiver, sendQ);
            keyReceiver.drainerDone();
            valueReceiver.drainerDone();
        } catch (Throwable t) {
            keyReceiver.drainerFailed(t);
            valueReceiver.drainerFailed(t);
            sneakyThrow(t);
        } finally {
            if (!submitterGoneSent) {
                try {
                    keyHandleSender.submit(sendQ, keyHandleSender.submitterGoneItem());
                } catch (Exception ignored) { }
            }
        }
    }

    private void mainLoop(
            ConcurrentConveyor<RestartItem> keyReceiver, ConcurrentConveyor<RestartItem> valueReceiver,
            AbstractConcurrentArrayQueue<RestartItem> sendQ
    ) {
        final int capacity = keyReceiver.queue(0).capacity();
        final Deque<RestartItem> keyDrain = new ArrayDeque<RestartItem>(capacity);
        final List<RestartItem> valueDrain = new ArrayList<RestartItem>(capacity);
        RestartItem danglingDownstreamItem = null;
        long idleCount = 0;
        boolean didWork = true;
        long keyCount = 0;
        long keyDrainCount = 0;
        while (!interrupted()) {
            if (didWork) {
                idleCount = 0;
            } else {
                IDLER.idle(idleCount++);
            }
            didWork = false;
            valueDrain.clear();
            valueReceiver.drainTo(valueDrain);
            for (RestartItem item : valueDrain) {
                consumeValueItem(item);
                didWork = true;
            }
            final int count = keyReceiver.drainTo(keyDrain, capacity - keyDrain.size());
            // Maintain counters used to determine the mean queue size over the whole run:
            if (count > 0) {
                keyCount += count;
                keyDrainCount++;
            }

            if (danglingDownstreamItem != null) {
                if (keyHandleSender.offer(sendQ, danglingDownstreamItem)) {
                    danglingDownstreamItem = null;
                } else {
                    // Don't drain more key items until the dangling item's submission succeeds
                    continue;
                }
            }
            for (RestartItem item; (item = keyDrain.pollFirst()) != null; ) {
                final RestartItem downstreamItem = consumeKeyItem(item);
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
        logger.fine(String.format("%s: drained %,d items, mean queue utilization was %.1f%% (capacity %,d)",
                currentThread().getName(), keyCount, 100.0 * keyCount / (keyDrainCount * capacity), capacity));
    }

    private RestartItem consumeKeyItem(RestartItem item) {
        if (!item.isSpecialItem()) {
            item.ramStore = reg.restartingRamStoreForPrefix(item.prefix);
            item.keyHandle = item.ramStore.toKeyHandle(item.key);
            return item;
        } else if (item.isClearedItem()) {
            return item;
        }
        assert item == keySubmitterGone;
        submitterGoneSent = true;
        return keyHandleSender.submitterGoneItem();
    }

    private void consumeValueItem(RestartItem item) {
        if (!(item instanceof WithSetOfKeyHandle)) {
            item.ramStore.accept(item.keyHandle, item.value);
        } else if (item != valueSubmitterGone) {
            final SetOfKeyHandle sokh = ((WithSetOfKeyHandle) item).sokh;
            reg.ramStoreForPrefix(item.prefix).removeNullEntries(sokh);
        } else {
            currentThread().interrupt();
        }
    }

    private static OneToOneConcurrentArrayQueue<RestartItem>[] makeQueues(int count) {
        final OneToOneConcurrentArrayQueue<RestartItem>[] qs = new OneToOneConcurrentArrayQueue[count];
        for (int i = 0; i < qs.length; i++) {
            qs[i] = new OneToOneConcurrentArrayQueue<RestartItem>(queueCapacity(count));
        }
        return qs;
    }

    private static ConcurrentConveyorSingleQueue<RestartItem>[] makeConveyors(int count) {
        final ConcurrentConveyorSingleQueue<RestartItem>[] conveyors = new ConcurrentConveyorSingleQueue[count];
        for (int i = 0; i < conveyors.length; i++) {
            conveyors[i] = concurrentConveyorSingleQueue(RestartItem.END,
                    new OneToOneConcurrentArrayQueue<RestartItem>(queueCapacity(count)));
        }
        return conveyors;
    }

    private static int queueCapacity(int queueCount) {
        return max(QUEUE_CAPACITY_BUDGET / queueCount, MIN_QUEUE_CAPACITY);
    }
}
