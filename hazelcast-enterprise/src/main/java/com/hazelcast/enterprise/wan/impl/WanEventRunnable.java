package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerOperation;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.util.Set;

/**
 * Class responsible for processing received {@link WanEvent}s
 * and notifying the {@link WanEventContainerOperation} of its completion.
 */
class WanEventRunnable extends AbstractWanEventRunnable {
    private final InternalWanEvent event;
    private final NodeEngine nodeEngine;
    private final Set<Operation> liveOperations;
    private final ILogger logger;

    WanEventRunnable(InternalWanEvent event,
                     WanEventContainerOperation operation,
                     int partitionId,
                     NodeEngine nodeEngine,
                     Set<Operation> liveOperations,
                     ILogger logger) {
        super(operation, partitionId);
        this.event = event;
        this.nodeEngine = nodeEngine;
        this.liveOperations = liveOperations;
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            final String serviceName = event.getServiceName();
            final WanSupportingService service = nodeEngine.getService(serviceName);
            service.onReplicationEvent(event, operation.getAcknowledgeType());
            operation.sendResponse(true);
        } catch (Exception e) {
            operation.sendResponse(false);
            log(logger, e);
        } finally {
            if (!liveOperations.remove(operation)) {
                logger.warning("Did not remove WanOperation from live operation list. Possible memory leak!");
            }
        }
    }
}
