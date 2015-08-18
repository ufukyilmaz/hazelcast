package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.LinkedList;

/**
 * No delaying distribution implementation on WAN replication
 */
public class WanNoDelayReplication extends AbstractWanReplication
        implements Runnable {

    private ILogger logger;
    private final LinkedList<WanReplicationEvent> failureQ = new LinkedList<WanReplicationEvent>();
    private volatile boolean running = true;

    @Override
    public void init(Node node, String groupName, String password, boolean snapshotEnabled,
                     String wanReplicationName, String... targets) {
        super.init(node, groupName, password, snapshotEnabled, wanReplicationName, targets);
        this.logger = node.getLogger(WanNoDelayReplication.class.getName());
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    public void shutdown() {
        running = false;
    }

    public void run() {
        while (running) {
            WanConnectionWrapper connectionWrapper = null;
            try {
                WanReplicationEvent event = (failureQ.size() > 0) ? failureQ.removeFirst() : stagingQueue.take();
                if (event != null) {
                    EnterpriseReplicationEventObject replicationEventObject
                            = (EnterpriseReplicationEventObject) event.getEventObject();
                    connectionWrapper = connectionManager.getConnection(getPartitionId(replicationEventObject.getKey()));
                    Connection conn = connectionWrapper.getConnection();
                    boolean eventSuccessfullySent = false;
                    if (conn != null && conn.isAlive()) {
                        try {
                            invokeOnWanTarget(conn.getEndPoint(), event).get();
                            eventSuccessfullySent = true;
                            removeReplicationEvent(event);
                        } catch (Exception ignored) {
                            logger.warning(ignored);
                        }
                    }
                    if (!eventSuccessfullySent) {
                        failureQ.add(event);
                    }

                }

            } catch (InterruptedException e) {
                running = false;
            } catch (Throwable e) {
                if (logger != null) {
                    logger.warning(e);
                }
                if (connectionWrapper != null) {
                    connectionManager.reportFailedConnection(connectionWrapper.getTargetAddress());
                }
            }
        }
    }
}
