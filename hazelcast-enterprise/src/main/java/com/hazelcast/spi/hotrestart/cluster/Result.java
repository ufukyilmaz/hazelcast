package com.hazelcast.spi.hotrestart.cluster;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result is a container to store result of an action in three states:
 * <ul>
 * <li>{@code PENDING}: means no final result yet</li>
 * <li>{@code FAILURE}: means action failed</li>
 * <li>{@code SUCCESS}: means action succeeded</li>
 * </ul>
 */
class Result {
    static final int PENDING = 0;
    static final int FAILURE = -1;
    static final int SUCCESS = 1;

    private final AtomicInteger value = new AtomicInteger(PENDING);

    boolean isSuccess() {
        return value.get() == SUCCESS;
    }

    boolean isFailure() {
        return value.get() == FAILURE;
    }

    boolean isPending() {
        return value.get() == PENDING;
    }

    int get() {
        return value.get();
    }

    void set(int newValue) {
        switch (newValue) {
            case PENDING:
            case FAILURE:
            case SUCCESS:
                value.set(newValue);
                return;

            default:
                throw new IllegalArgumentException();
        }
    }

    boolean cas(int expected, int newValue) {
        switch (newValue) {
            case PENDING:
            case FAILURE:
            case SUCCESS:
                return value.compareAndSet(expected, newValue);

            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return "Result: " + (isSuccess() ? "Success" : (isFailure() ? "Failure" : "Pending"));
    }
}
