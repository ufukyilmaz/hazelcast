package com.hazelcast.map.impl.querycache;

import com.hazelcast.map.impl.querycache.accumulator.AccumulatorProcessor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

/**
 * Scheduler abstraction to allow different type of implementations
 * on query cache subscriber and query cache publisher sides.
 * <p/>
 * Mainly used for offloading indexing-process and scheduling {@link AccumulatorProcessor}.
 */
public interface QueryCacheScheduler {

    /**
     * Executes a task.
     *
     * @param task task to execute.
     * @throws RejectedExecutionException if this task cannot be accepted for execution.
     */
    void execute(Runnable task);

    /**
     * Executes a task periodically with the supplied {@code delaySeconds}.
     *
     * @param task         task to execute.
     * @param delaySeconds the time between subsequent execution
     * @return a ScheduledFuture representing pending completion of
     * the task and whose <tt>get()</tt> method will return
     * <tt>null</tt> upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     */
    ScheduledFuture<?> scheduleAtFixedRateWithDelaySeconds(Runnable task, long delaySeconds);

    /**
     * @see java.util.concurrent.ScheduledExecutorService#schedule
     */
    ScheduledFuture<?> scheduleWithDelaySeconds(Runnable task, long delaySeconds);

    /**
     * Shuts down this scheduler.
     */
    void shutdown();
}
