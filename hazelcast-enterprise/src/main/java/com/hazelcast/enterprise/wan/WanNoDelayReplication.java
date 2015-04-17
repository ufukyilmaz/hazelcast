/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.enterprise.wan;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * No delaying distribution implementation on WAN replication
 */
public class WanNoDelayReplication
        implements Runnable, WanReplicationEndpoint {

    private String targetGroupName;
    private String localGroupName;
    private Node node;
    private ILogger logger;
    private final LinkedList<WanReplicationEvent> failureQ = new LinkedList<WanReplicationEvent>();
    private BlockingQueue<WanReplicationEvent> eventQueue;
    private volatile boolean running = true;
    private WanConnectionManager connectionManager;

    public void init(Node node, String groupName, String password, String... targets) {
        this.node = node;
        this.targetGroupName = groupName;
        this.logger = node.getLogger(WanNoDelayReplication.class.getName());

        int queueSize = node.getGroupProperties().ENTERPRISE_WAN_REP_QUEUESIZE.getInteger();
        localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();
        this.eventQueue = new ArrayBlockingQueue<WanReplicationEvent>(queueSize);

        connectionManager = new WanConnectionManager(node);
        connectionManager.init(groupName, password, Arrays.asList(targets));

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

            //the replication event could not be published because the eventQueue is full. So we are going
            //to drain one item and then offer it again.
            //todo: isn't it dangerous to drop a ReplicationEvent?
            eventQueue.poll();
            logger.warning("Event queue is full, dropping an event." + replicationEvent);

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

                if (conn != null && conn.isAlive()) {
                    Data data = node.nodeEngine.getSerializationService().toData(event);
                    Packet packet = new Packet(data);
                    packet.setHeader(Packet.HEADER_WAN_REPLICATION);
                    node.nodeEngine.getPacketTransceiver().transmit(packet, conn);
                } else {
                    failureQ.addFirst(event);
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

    private int getPartitionId(Object key) {
        return  node.nodeEngine.getPartitionService().getPartitionId(key);
    }
}
