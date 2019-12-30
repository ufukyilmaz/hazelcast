package com.hazelcast.wan.custom;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.wan.WanConsumer;
import com.hazelcast.wan.impl.InternalWanEvent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CustomWanConsumer implements WanConsumer, Runnable, HazelcastInstanceAware {

    private volatile boolean running = true;
    private Node node;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.node = ((HazelcastInstanceImpl) hazelcastInstance).node;
    }

    @Override
    public void init(String wanReplicationName, WanConsumerConfig config) {
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
                InternalWanEvent event = CustomWanPublisher.EVENT_QUEUE.poll(100, MILLISECONDS);
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
