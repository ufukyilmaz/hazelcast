package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.WanOperation;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;

import java.util.concurrent.TimeUnit;

/**
 * Base class for implementations processing received WAN events
 */
public abstract class AbstractWanEventRunnable implements StripedRunnable, TimeoutRunnable {
    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    protected final WanOperation operation;
    private final int partitionId;

    AbstractWanEventRunnable(WanOperation operation, int partitionId) {
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
}
