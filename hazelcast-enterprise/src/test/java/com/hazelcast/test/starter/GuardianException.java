package com.hazelcast.test.starter;

public class GuardianException extends RuntimeException {

    public GuardianException(String message) {
        super(message);
    }

    public GuardianException(Throwable t) {
        super(t);
    }

    public GuardianException(String message, Throwable cause) {
        super(message, cause);
    }
}
