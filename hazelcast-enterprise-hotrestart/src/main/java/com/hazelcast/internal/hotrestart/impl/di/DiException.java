package com.hazelcast.internal.hotrestart.impl.di;

/**
 * Thrown by the {@link DiContainer}
 */
public class DiException extends RuntimeException {

    public DiException(String message) {
        super(message);
    }

    public DiException(String message, Throwable cause) {
        super(message, cause);
    }
}
