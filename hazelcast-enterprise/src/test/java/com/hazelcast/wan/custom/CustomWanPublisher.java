package com.hazelcast.wan.custom;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.impl.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.impl.WanSyncEvent;
import com.hazelcast.enterprise.wan.impl.replication.AbstractWanPublisher;
import com.hazelcast.enterprise.wan.impl.replication.WanPublisherSyncSupport;
import com.hazelcast.map.impl.wan.EnterpriseMapReplicationObject;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.wan.impl.ConsistencyCheckResult;
import com.hazelcast.wan.impl.WanAntiEntropyEvent;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.impl.WanSyncStats;
import com.hazelcast.wan.impl.InternalWanReplicationEvent;

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
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig targetClusterConfig) {
        super.init(wanReplicationConfig, targetClusterConfig);
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
                ArrayList<InternalWanReplicationEvent> batchList = new ArrayList<>(batchSize);

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
