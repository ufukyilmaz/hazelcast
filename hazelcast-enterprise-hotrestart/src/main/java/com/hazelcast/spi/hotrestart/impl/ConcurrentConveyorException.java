package com.hazelcast.spi.hotrestart.impl;

/**
 * Exception thrown by the {@link ConcurrentConveyor}.
 */
public class ConcurrentConveyorException extends RuntimeException {
    public ConcurrentConveyorException(String message) {
        super(message);
    }
    public ConcurrentConveyorException(String message, Throwable cause) {
        super(message, cause);
    }
}
