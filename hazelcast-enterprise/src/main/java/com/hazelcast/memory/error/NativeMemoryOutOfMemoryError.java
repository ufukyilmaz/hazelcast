package com.hazelcast.memory.error;

public class NativeMemoryOutOfMemoryError extends NativeMemoryError {

    public NativeMemoryOutOfMemoryError() {
    }

    public NativeMemoryOutOfMemoryError(String message) {
        super(message);
    }

    public NativeMemoryOutOfMemoryError(String message, Throwable cause) {
        super(message, cause);
    }
}
