package com.hazelcast.wan.custom;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
import com.hazelcast.instance.Node;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by emrah on 08/03/16.
 */
public class CustomWanPublisher extends AbstractWanPublisher implements Runnable {

    public static final BlockingQueue<WanReplicationEvent> EVENT_QUEUE = new ArrayBlockingQueue<WanReplicationEvent>(100);
    private volatile boolean running = true;

    @Override
    public void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig targetClusterConfig) {
        super.init(node, wanReplicationConfig, targetClusterConfig);
        node.nodeEngine.getExecutionService().execute("hz:custom:wan:publisher", this);
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void run() {
        while (running) {
            try {
                WanReplicationEvent event = stagingQueue.take();
                EVENT_QUEUE.offer(event);
                removeReplicationEvent(event);
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        running = false;
    }
}
