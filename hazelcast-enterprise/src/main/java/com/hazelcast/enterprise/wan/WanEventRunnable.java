package com.hazelcast.enterprise.wan;

import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Set;

/**
 * Class responsible for processing received {@link WanReplicationEvent}s
 * and notifying the {@link WanOperation} of its completion.
 */
class WanEventRunnable extends AbstractWanEventRunnable {
    private final WanReplicationEvent event;
    private final NodeEngine nodeEngine;
    private final Set<Operation> liveOperations;
    private final ILogger logger;

    WanEventRunnable(WanReplicationEvent event,
                     WanOperation operation,
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
            final ReplicationSupportingService service = nodeEngine.getService(serviceName);
            event.setAcknowledgeType(operation.getAcknowledgeType());
            service.onReplicationEvent(event);
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
