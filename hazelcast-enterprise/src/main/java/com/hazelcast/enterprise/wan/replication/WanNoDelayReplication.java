package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseReplicationEventObject;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.connection.WanConnectionWrapper;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * No delaying distribution implementation on WAN replication
 */
public class WanNoDelayReplication extends AbstractWanReplication
        implements Runnable, WanReplicationEndpoint {

    private ILogger logger;
    private BlockingQueue<WanReplicationEvent> eventQueue;
    private final LinkedList<WanReplicationEvent> failureQ = new LinkedList<WanReplicationEvent>();
    private volatile boolean running = true;

    @Override
    public void init(Node node, String groupName, String password, boolean snapshotEnabled, String... targets) {
        super.init(node, groupName, password, snapshotEnabled, targets);
        this.logger = node.getLogger(WanNoDelayReplication.class.getName());
        this.eventQueue = new ArrayBlockingQueue<WanReplicationEvent>(queueSize);
        node.nodeEngine.getExecutionService().execute("hz:wan", this);
    }

    @Override
    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        WanReplicationEvent replicationEvent = new WanReplicationEvent(serviceName, eventObject);

        EnterpriseReplicationEventObject replicationEventObject = (EnterpriseReplicationEventObject) eventObject;
        if (!replicationEventObject.getGroupNames().contains(targetGroupName)) {
            replicationEventObject.getGroupNames().add(localGroupName);

            //if the replication event is published, we are done.
            if (eventQueue.offer(replicationEvent)) {
                return;
            }

            long curTime = System.currentTimeMillis();
            if (curTime > lastQueueFullLogTimeMs + queueLoggerTimePeriodMs) {
                lastQueueFullLogTimeMs = curTime;
                logger.severe("Wan replication event queue is full. Dropping events.");
            } else {
                logger.finest("Wan replication event queue is full. An event is dropped.");
            }

            //the replication event could not be published because the eventQueue is full. So we are going
            //to drain one item and then offer it again.
            eventQueue.poll();

            if (!eventQueue.offer(replicationEvent)) {
                logger.warning("Could not publish replication event: " + replicationEvent);
            }
        }
    }

    public void shutdown() {
        running = false;
    }

    public void run() {
        while (running) {
            WanConnectionWrapper connectionWrapper = null;
            try {
                WanReplicationEvent event = (failureQ.size() > 0) ? failureQ.removeFirst() : eventQueue.take();
                EnterpriseReplicationEventObject replicationEventObject
                        = (EnterpriseReplicationEventObject) event.getEventObject();
                connectionWrapper = connectionManager.getConnection(getPartitionId(replicationEventObject.getKey()));
                Connection conn = connectionWrapper.getConnection();
                boolean eventSuccessfullySent = false;
                if (conn != null && conn.isAlive()) {
                    try {
                        invokeOnWanTarget(conn.getEndPoint(), event).get();
                        eventSuccessfullySent = true;
                    } catch (Exception ignored) {
                        logger.warning(ignored);
                    }
                }
                if (!eventSuccessfullySent) {
                    failureQ.add(event);
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
