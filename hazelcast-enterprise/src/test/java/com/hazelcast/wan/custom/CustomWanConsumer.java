package com.hazelcast.wan.custom;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.wan.WanReplicationConsumer;
import com.hazelcast.wan.WanReplicationEvent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CustomWanConsumer implements WanReplicationConsumer, Runnable {

    private volatile boolean running = true;
    private Node node;

    @Override
    public void init(Node node, String wanReplicationName, WanConsumerConfig config) {
        this.node = node;
        node.getNodeEngine().getExecutionService().execute("hz:wan:custom:consumer", this);
    }

    @Override
    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            try {
                WanReplicationEvent event = CustomWanPublisher.EVENT_QUEUE.poll(100, MILLISECONDS);
                if (event != null) {
                    EnterpriseWanReplicationService wanRepService
                            = (EnterpriseWanReplicationService) node.nodeEngine.getWanReplicationService();
                    wanRepService.handleEvent(event, WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
