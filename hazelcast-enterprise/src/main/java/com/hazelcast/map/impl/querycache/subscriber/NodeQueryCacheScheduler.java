package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.ExecutorType;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Node side implementation of {@code QueryCacheScheduler}.
 *
 * @see QueryCacheScheduler
 */
public class NodeQueryCacheScheduler implements QueryCacheScheduler {

    /**
     * Prefix for #executorName.
     */
    private static final String EXECUTOR_NAME_PREFIX = "hz:scheduled:cqc:";

    /**
     * Default work-queue capacity for this executor.
     */
    private static final int EXECUTOR_DEFAULT_QUEUE_CAPACITY = 10000;

    private final String executorName;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutionService executionService;

    public NodeQueryCacheScheduler(EnterpriseMapServiceContext mapServiceContext) {
        executionService = getExecutionService(mapServiceContext);
        executorName = EXECUTOR_NAME_PREFIX + UUID.randomUUID().toString();
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        scheduledExecutor = executionService.getScheduledExecutor(executorName);
    }

    private ExecutionService getExecutionService(EnterpriseMapServiceContext mapServiceContext) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRateWithDelaySeconds(Runnable task, long delaySeconds) {
        return scheduledExecutor.scheduleAtFixedRate(task, 1, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleWithDelaySeconds(Runnable task, long delaySeconds) {
        return scheduledExecutor.schedule(task, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Runnable task) {
        scheduledExecutor.execute(task);
    }

    @Override
    public void shutdown() {
        executionService.shutdownExecutor(executorName);
    }
}
