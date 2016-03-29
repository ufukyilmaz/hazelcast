package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.LinkedList;

/**
 * No delaying distribution implementation on WAN replication.
 */
public class WanNoDelayReplication
        extends AbstractWanReplication
        implements Runnable {

    private static final int STAGING_QUEUE_SIZE = 50;
    private final LinkedList<WanReplicationEvent> failureQ = new LinkedList<WanReplicationEvent>();

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig targetClusterConfig) {
        super.init(node, wanReplicationConfig, targetClusterConfig);
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    public void run() {
        while (running) {
            WanConnectionWrapper connectionWrapper = null;
            try {
                WanReplicationEvent event =
                        (failureQ.size() > 0)
                                ? failureQ.removeFirst()
                                : stagingQueue.take();
                if (event != null) {
                    EnterpriseReplicationEventObject replicationEventObject
                            = (EnterpriseReplicationEventObject) event.getEventObject();
                    int partitionId = getPartitionId(replicationEventObject.getKey());
                    connectionWrapper = connectionManager.getConnection(partitionId);
                    Connection conn = connectionWrapper.getConnection();
                    handleEvent(event, conn);
                }
            } catch (Throwable t) {
                logger.warning(t);
                if (connectionWrapper != null) {
                    connectionManager.reportFailedConnection(connectionWrapper.getTargetAddress());
                }
            }
        }
    }

    private void handleEvent(WanReplicationEvent event, Connection conn) {
        boolean eventSuccessfullySent = false;
        try {
            if (conn != null && conn.isAlive()) {
                boolean isTargetInvocationSuccessful = invokeOnWanTarget(conn.getEndPoint(), event);
                if (isTargetInvocationSuccessful) {
                    removeReplicationEvent(event);
                }
                eventSuccessfullySent = isTargetInvocationSuccessful;
            }
        } finally {
            if (!eventSuccessfullySent) {
                failureQ.add(event);
            }
        }
    }

    @Override
    public int getStagingQueueSize() {
        return STAGING_QUEUE_SIZE;
    }
}
