package com.hazelcast.wan.custom;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.WanSyncEvent;
import com.hazelcast.enterprise.wan.impl.replication.AbstractWanPublisher;
import com.hazelcast.enterprise.wan.impl.replication.WanPublisherSyncSupport;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEvent;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.ConsistencyCheckResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CustomWanPublisher extends AbstractWanPublisher implements Runnable {

    static final BlockingQueue<WanReplicationEvent> EVENT_QUEUE = new ArrayBlockingQueue<>(100);

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
    public void publishAntiEntropyEvent(WanAntiEntropyEvent event) {

    }

    @Override
    protected WanPublisherSyncSupport createWanSyncSupport() {
        return new NoOpSyncSupport();
    }

    @Override
    public void run() {
        while (running) {
            try {
                int batchSize = configurationContext.getBatchSize();
                ArrayList<WanReplicationEvent> batchList = new ArrayList<>(batchSize);

                for (IPartition partition : node.getPartitionService().getPartitions()) {
                    if (partition.isLocal()) {
                        batchList.clear();
                        eventQueueContainer.drainRandomWanQueue(partition.getPartitionId(), batchList, batchSize);
                        for (WanReplicationEvent event : batchList) {
                            if (event != null) {
                                EVENT_QUEUE.put(event);
                            }
                        }
                        finalizeWanEventReplication(batchList);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class NoOpSyncSupport implements WanPublisherSyncSupport {
        @Override
        public void destroyMapData(String mapName) {

        }

        @Override
        public void processEvent(WanSyncEvent event) {

        }

        @Override
        public void processEvent(WanConsistencyCheckEvent event) {

        }

        @Override
        public Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, WanSyncStats> getLastSyncStats() {
            return Collections.emptyMap();
        }

        @Override
        public void removeReplicationEvent(EnterpriseMapReplicationObject sync) {

        }
    }

    @Override
    protected void afterShutdown() {
        running = false;
    }
}
