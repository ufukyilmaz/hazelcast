package com.hazelcast.wan.custom;

import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationConsumer;
import com.hazelcast.instance.Node;
import com.hazelcast.wan.WanReplicationEvent;

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
                WanReplicationEvent event = com.hazelcast.wan.custom.CustomWanPublisher.EVENT_QUEUE.take();
                EnterpriseWanReplicationService wanRepService
                        = (EnterpriseWanReplicationService) node.nodeEngine.getWanReplicationService();
                wanRepService.handleEvent(event);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
