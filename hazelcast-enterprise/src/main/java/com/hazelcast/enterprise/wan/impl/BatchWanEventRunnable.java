package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.replication.BatchWanReplicationEvent;
import com.hazelcast.enterprise.wan.impl.operation.WanOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Set;

/**
 * Class responsible for processing received {@link BatchWanReplicationEvent}s
 * and notifying the {@link WanOperation} of its completion.
 */
class BatchWanEventRunnable extends AbstractWanEventRunnable {
    private final BatchWanReplicationEvent batchEvent;
    private final NodeEngine nodeEngine;
    private final Set<Operation> liveOperations;
    private final ILogger logger;

    BatchWanEventRunnable(BatchWanReplicationEvent batchEvent,
                          WanOperation operation,
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
            for (WanReplicationEvent wanReplicationEvent : batchEvent.getEvents()) {
                String serviceName = wanReplicationEvent.getServiceName();
                ReplicationSupportingService service = nodeEngine.getService(serviceName);
                wanReplicationEvent.setAcknowledgeType(operation.getAcknowledgeType());
                service.onReplicationEvent(wanReplicationEvent);
            }
            operation.sendResponse(true);
        } catch (Exception e) {
            operation.sendResponse(false);
            logger.severe(e);
        } finally {
            if (!liveOperations.remove(operation)) {
                logger.warning("Did not remove WanOperation from live operation list. Possible memory leak!");
            }
        }
    }
}
