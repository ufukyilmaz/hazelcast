package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerOperation;
import com.hazelcast.enterprise.wan.impl.replication.WanEventBatch;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.impl.InternalWanEvent;

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
    private final WanAcknowledger acknowledger;

    BatchWanEventRunnable(WanEventBatch batchEvent,
                          WanEventContainerOperation operation,
                          int partitionId,
                          NodeEngine nodeEngine,
                          Set<Operation> liveOperations,
                          ILogger logger,
                          WanAcknowledger acknowledger) {
        super(operation, partitionId);
        this.batchEvent = batchEvent;
        this.nodeEngine = nodeEngine;
        this.liveOperations = liveOperations;
        this.logger = logger;
        this.acknowledger = acknowledger;
    }

    @Override
    public void run() {
        try {
            for (InternalWanEvent wanEvent : batchEvent.getEvents()) {
                String serviceName = wanEvent.getServiceName();
                WanSupportingService service = nodeEngine.getService(serviceName);
                service.onReplicationEvent(wanEvent, operation.getAcknowledgeType());
            }
            acknowledger.acknowledgeSuccess(operation);
        } catch (Exception e) {
            acknowledger.acknowledgeFailure(operation);
            log(logger, e);
        } finally {
            if (!liveOperations.remove(operation)) {
                logger.warning("Did not remove WanOperation from live operation list. Possible memory leak!");
            }
        }
    }
}
