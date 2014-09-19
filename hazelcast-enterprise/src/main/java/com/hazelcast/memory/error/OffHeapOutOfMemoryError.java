package com.hazelcast.memory.error;

public class OffHeapOutOfMemoryError extends OffHeapError {

    public OffHeapOutOfMemoryError() {
    }

    public OffHeapOutOfMemoryError(String message) {
        super(message);
    }

    public OffHeapOutOfMemoryError(String message, Throwable cause) {
        super(message, cause);
    }
}
