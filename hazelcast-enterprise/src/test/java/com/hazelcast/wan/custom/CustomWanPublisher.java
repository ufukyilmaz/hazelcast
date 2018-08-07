package com.hazelcast.wan.custom;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
import com.hazelcast.instance.Node;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CustomWanPublisher extends AbstractWanPublisher implements Runnable {

    static final BlockingQueue<WanReplicationEvent> EVENT_QUEUE = new ArrayBlockingQueue<WanReplicationEvent>(100);

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
                WanReplicationEvent event = stagingQueue.poll(100, MILLISECONDS);
                if (event != null) {
                    EVENT_QUEUE.put(event);
                    removeReplicationEvent(event);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void afterShutdown() {
        running = false;
    }
}
