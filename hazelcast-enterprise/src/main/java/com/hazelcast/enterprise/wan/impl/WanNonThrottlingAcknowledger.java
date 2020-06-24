package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * {@link WanAcknowledger} implementation that acknowledges immediately
 * without adding any delay or performing any other operation before
 * performing the acknowledgement. This implementation is a fallback
 * implementation to {@link WanThrottlingAcknowledger} in case that one
 * for any reason is desired to be turned off. Setting this acknowledger
 * restores the behavior as it was before introducing the
 * {@link WanAcknowledger} abstraction.
 */
public class WanNonThrottlingAcknowledger implements WanAcknowledger {

    public WanNonThrottlingAcknowledger(Node node) {
        ILogger logger = node.getLogger(WanNonThrottlingAcknowledger.class);
        logger.info("Using non-throttling WAN acknowledgement strategy");
    }

    @Override
    public void acknowledgeSuccess(Operation operation) {
        acknowledge(operation, true);
    }

    @Override
    public void acknowledgeFailure(Operation operation) {
        acknowledge(operation, false);
    }

    private void acknowledge(Operation operation, boolean success) {
        operation.sendResponse(success);
    }
}
