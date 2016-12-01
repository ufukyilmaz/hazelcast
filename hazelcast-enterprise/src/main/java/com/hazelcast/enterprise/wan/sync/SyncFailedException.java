package com.hazelcast.enterprise.wan.sync;

/**
 * A SyncFailedException is thrown when a WAN Sync operation is failed due to any reason
 */
public class SyncFailedException extends RuntimeException {

    public SyncFailedException(String s) {
        super(s);
    }

    public SyncFailedException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
