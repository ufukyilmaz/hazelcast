package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ExecutorType;

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
    private final TaskScheduler taskScheduler;
    private final ExecutionService executionService;

    public NodeQueryCacheScheduler(EnterpriseMapServiceContext mapServiceContext) {
        executionService = getExecutionService(mapServiceContext);
        executorName = EXECUTOR_NAME_PREFIX + UuidUtil.newUnsecureUuidString();
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        taskScheduler = executionService.getTaskScheduler(executorName);
    }

    private ExecutionService getExecutionService(EnterpriseMapServiceContext mapServiceContext) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable task, long delaySeconds) {
        return taskScheduler.scheduleWithRepetition(task, 1, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Runnable task) {
        taskScheduler.execute(task);
    }

    @Override
    public void shutdown() {
        executionService.shutdownExecutor(executorName);
    }
}
