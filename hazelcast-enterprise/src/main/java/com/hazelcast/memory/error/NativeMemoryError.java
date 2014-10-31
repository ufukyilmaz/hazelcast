package com.hazelcast.memory.error;

public class NativeMemoryError extends Error {

    public NativeMemoryError() {
        super();
    }

    public NativeMemoryError(String message, Throwable cause) {
        super(message, cause);
    }

    public NativeMemoryError(String message) {
        super(message);
    }
}
