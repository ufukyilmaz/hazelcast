package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * WAN acknowledgment strategy interface.
 */
public interface WanAcknowledger {
    /**
     * Acknowledges the WAN operation as successful operation.
     *
     * @param operation the WAN operation to acknowledge
     */
    void acknowledgeSuccess(Operation operation);

    /**
     * Acknowledges the WAN operation as failed operation.
     *
     * @param operation the WAN operation to acknowledge
     */
    void acknowledgeFailure(Operation operation);
}
