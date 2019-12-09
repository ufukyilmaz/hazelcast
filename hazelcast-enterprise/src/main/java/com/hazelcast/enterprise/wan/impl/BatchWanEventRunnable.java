package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerOperation;
import com.hazelcast.enterprise.wan.impl.replication.WanEventBatch;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanEvent;

import java.util.Set;

/**
 * Class responsible for processing received {@link WanEventBatch}s
 * and notifying the {@link WanEventContainerOperation} of its completion.
 */
class BatchWanEventRunnable extends AbstractWanEventRunnable {
    private final WanEventBatch batchEvent;
    private final NodeEngine nodeEngine;
    private final Set<Operation> liveOperations;
    private final ILogger logger;

    BatchWanEventRunnable(WanEventBatch batchEvent,
                          WanEventContainerOperation operation,
                          int partitionId,
                          NodeEngine nodeEngine,
                          Set<Operation> liveOperations,
                          ILogger logger) {
        super(operation, partitionId);
        this.batchEvent = batchEvent;
        this.nodeEngine = nodeEngine;
        this.liveOperations = liveOperations;
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            for (WanEvent wanEvent : batchEvent.getEvents()) {
                String serviceName = wanEvent.getServiceName();
                WanSupportingService service = nodeEngine.getService(serviceName);
                service.onReplicationEvent(wanEvent, operation.getAcknowledgeType());
            }
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
