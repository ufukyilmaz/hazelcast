package com.hazelcast.enterprise.wan.impl.connection;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown if member was unable to connect to a WAN endpoint in
 * the target cluster or the WAN protocol negotiation failed.
 */
class WanConnectionException extends HazelcastException {
    WanConnectionException(String message) {
        super(message);
    }

    WanConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
