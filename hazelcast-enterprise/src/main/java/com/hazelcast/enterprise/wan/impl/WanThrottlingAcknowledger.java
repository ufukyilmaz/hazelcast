package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.properties.ClusterProperty.WAN_CONSUMER_ACK_DELAY_BACKOFF_INIT_MS;
import static com.hazelcast.spi.properties.ClusterProperty.WAN_CONSUMER_ACK_DELAY_BACKOFF_MAX_MS;
import static com.hazelcast.spi.properties.ClusterProperty.WAN_CONSUMER_ACK_DELAY_BACKOFF_MULTIPLIER;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * {@link WanAcknowledger} implementation that is meant to be used for
 * throttling the WAN event stream on the consumer side. Throttling is
 * done by testing the health condition of the given WAN consumer member
 * to prevent it getting overloaded by the WAN publisher. The health
 * condition check is performed by comparing the number of the pending
 * invocations on the consumer node against a configurable threshold. If
 * the threshold is exceeded the acknowledgement is delayed and the
 * condition will be evaluated later again. Since the publisher
 * waits with sending the next batch to the same WAN target endpoint
 * until the previously sent one is acknowledged, this implementation
 * slows down the publisher to the desired level.
 */
public class WanThrottlingAcknowledger implements WanAcknowledger {
    private static final int THRESHOLD_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);

    private final int invocationThreshold;
    private final Node node;
    private final int backoffInit;
    private final int backoffMax;
    private final float backoffMultiplier;
    private final Object queueLock = new Object();
    private final ILogger logger;
    // guarded by queueLock
    private final Queue<DelayedAcknowledgment> delayedAcknowledgments = new LinkedList<>();
    private final AtomicReference<DelayingState> delayingState = new AtomicReference<>(DelayingState.NOT_DELAYING);
    private InvocationRegistry invocationRegistry;
    private TaskScheduler wanScheduler;
    private volatile long lastThresholdLogMs;

    public WanThrottlingAcknowledger(Node node, int invocationThreshold) {
        this.invocationThreshold = invocationThreshold;
        this.node = node;
        HazelcastProperties properties = node.getProperties();
        this.backoffInit = properties.getInteger(WAN_CONSUMER_ACK_DELAY_BACKOFF_INIT_MS);
        this.backoffMax = properties.getInteger(WAN_CONSUMER_ACK_DELAY_BACKOFF_MAX_MS);
        this.backoffMultiplier = properties.getFloat(WAN_CONSUMER_ACK_DELAY_BACKOFF_MULTIPLIER);
        this.logger = node.getLogger(WanThrottlingAcknowledger.class);
        logger.info("Using throttling WAN acknowledgement strategy with pending invocation threshold " + invocationThreshold);
    }

    @Override
    public void acknowledgeSuccess(Operation operation) {
        acknowledge(operation, true);
    }

    @Override
    public void acknowledgeFailure(Operation operation) {
        acknowledge(operation, false);
    }

    private void acknowledge(Operation operation, boolean success) {
        // we check the delaying state here as well
        // if we already delay, we need to handle this acknowledgement
        // the same way we handle the delayed ones to prevent being
        // biased in favor of the current operation
        int pendingInvocations = invocationRegistry().size();
        boolean thresholdExceeded = pendingInvocations >= invocationThreshold;
        if (!thresholdExceeded && delayingState.get() != DelayingState.DELAYING) {
            operation.sendResponse(success);
        } else {
            final boolean shouldScheduleTask;
            synchronized (queueLock) {
                // lock contention is very unlikely
                // here we are already in the "unhealthy" case where we
                // started delaying the WAN publishers, which means the
                // WAN sources will hold back with sending the batches
                delayedAcknowledgments.offer(new DelayedAcknowledgment(operation, success));
                shouldScheduleTask = delayingState.compareAndSet(DelayingState.NOT_DELAYING, DelayingState.DELAYING);
                long curTime = System.currentTimeMillis();

                if (thresholdExceeded) {
                    String logMessage = String.format("Pending invocation threshold exceeded, delaying WAN acknowledgments. "
                            + "Pending invocations: %d, threshold: %d", pendingInvocations, invocationThreshold);
                    if (curTime > lastThresholdLogMs + THRESHOLD_LOGGER_PERIOD_MILLIS) {
                        lastThresholdLogMs = curTime;
                        logger.warning(logMessage);
                    } else if (logger.isFinestEnabled()) {
                        logger.finest(logMessage);
                    }
                }
            }

            if (shouldScheduleTask) {
                new HealthConditionCheckTask().schedule();
            }
        }
    }

    private InvocationRegistry invocationRegistry() {
        if (invocationRegistry != null) {
            return invocationRegistry;
        }

        invocationRegistry = node.getNodeEngine().getOperationService().getInvocationRegistry();
        return invocationRegistry;
    }

    private final class HealthConditionCheckTask implements Runnable {
        private long delayMicros = -1;

        @Override
        public void run() {
            if (invocationRegistry.size() < invocationThreshold) {
                ArrayList<DelayedAcknowledgment> acks = new ArrayList<>(delayedAcknowledgments.size());
                synchronized (queueLock) {
                    DelayedAcknowledgment delayedAck;
                    while ((delayedAck = delayedAcknowledgments.poll()) != null) {
                        acks.add(delayedAck);
                    }
                    delayingState.set(DelayingState.NOT_DELAYING);
                }
                Iterator<DelayedAcknowledgment> acksIter = acks.iterator();
                while (acksIter.hasNext()) {
                    wanScheduler().execute(acksIter.next());
                }
            } else {
                schedule();
            }
        }

        public void schedule() {
            wanScheduler().schedule(this, delay(), MICROSECONDS);
        }

        private long delay() {
            if (delayMicros > 0) {
                // We expect that exceeding the invocation threshold is
                // caught early and that the recovery is fast, so we
                // re-evaluate the health condition frequently in the
                // beginning to prevent slowing down the WAN publisher
                // for too long.
                delayMicros = (long) min(delayMicros * backoffMultiplier, backoffMax);
            } else {
                delayMicros = backoffInit;
            }
            return delayMicros;
        }

        private TaskScheduler wanScheduler() {
            if (wanScheduler != null) {
                return wanScheduler;
            }

            wanScheduler = node.getNodeEngine().getExecutionService().getTaskScheduler("wan-ack-throttle");
            return wanScheduler;
        }
    }

    private static final class DelayedAcknowledgment implements Runnable {
        private final Operation operation;
        private final boolean success;

        private DelayedAcknowledgment(Operation operation, boolean success) {
            this.operation = operation;
            this.success = success;
        }

        @Override
        public void run() {
            operation.sendResponse(success);
        }
    }

    private enum DelayingState {
        /**
         * There is no task scheduled for checking the health condition
         * at a later time.
         */
        NOT_DELAYING,
        /**
         * There is a task scheduled for checking the health condition
         * at a later time.
         */
        DELAYING
    }
}
