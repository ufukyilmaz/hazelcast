package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.internal.util.executor.TimeoutRunnable;

import java.util.concurrent.TimeUnit;

/**
 * Base class for implementations processing received WAN events
 */
public abstract class AbstractWanEventRunnable implements StripedRunnable, TimeoutRunnable {
    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    protected final WanEventContainerOperation operation;
    private final int partitionId;

    AbstractWanEventRunnable(WanEventContainerOperation operation, int partitionId) {
        this.operation = operation;
        this.partitionId = partitionId;
    }

    @Override
    public int getKey() {
        return partitionId;
    }

    @Override
    public long getTimeout() {
        return STRIPED_RUNNABLE_TIMEOUT_SECONDS;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.SECONDS;
    }

    // convenience method for logging exceptions, allowing for custom handling of specific exception types
    void log(ILogger logger, Throwable t) {
        if (t instanceof CacheNotExistsException) {
            // log just the message for CacheNotExistsException because a) when this exception occurs,
            // it may occur frequently, and b) the message is informative enough
            logger.severe(t.getMessage());
        } else {
            logger.severe(t);
        }
    }
}
