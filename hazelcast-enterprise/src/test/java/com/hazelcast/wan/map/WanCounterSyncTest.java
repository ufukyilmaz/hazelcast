package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.EmptyStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class WanCounterSyncTest extends MapWanReplicationTestSupport {
    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";
    private static final String TARGET_GROUP_NAME = "B";

    private ScheduledExecutorService executorService;

    @Before
    public void setUp() {
        setupReplicateFrom(configA, configB, singleNodeB.length, REPLICATION_NAME, PassThroughMergePolicy.class.getName());
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void cleanup() {
        super.cleanup();
        executorService.shutdown();
    }

    @Test
    public void testCountersReachZeroAfterSyncingInParallelWithLoad() throws Exception {
        initCluster(singleNodeA, configA);
        initCluster(singleNodeB, configB);

        executorService.scheduleAtFixedRate(new SyncTask(), 10, 100, MILLISECONDS);
        executorService.submit(new LoadTask())
                       .get();

        verifyEventCountersAreEventuallyZero(clusterA);
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    private class LoadTask implements Runnable {
        @Override
        public void run() {
            final IMap<Object, Object> map = singleNodeA[0].getMap(MAP_NAME);
            for (int i = 0; i < 1000; i++) {
                if (i % 10 == 0) {
                    map.put(i, i);
                    try {
                        MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        EmptyStatement.ignore(e);
                    }
                }
            }
        }
    }

    private class SyncTask implements Runnable {
        @Override
        public void run() {
            try {
                EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(clusterA[0]);
                wanReplicationService.syncMap(REPLICATION_NAME, TARGET_GROUP_NAME, MAP_NAME);
            } catch (Exception ex) {
                EmptyStatement.ignore(ex);
            }
        }
    }

    private void verifyEventCountersAreEventuallyZero(final HazelcastInstance[] cluster) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : cluster) {
                    if (instance != null && instance.getLifecycleService().isRunning()) {
                        int outboundQueueSize = getPrimaryOutboundQueueSize(instance);

                        WanBatchReplication endpoint = getWanReplicationEndpoint(instance);
                        int outboundBackupQueueSize = endpoint.getCurrentBackupElementCount();

                        assertEquals(0, outboundQueueSize);
                        assertEquals(0, outboundBackupQueueSize);
                    }
                }
            }
        });
    }

    private int getPrimaryOutboundQueueSize(HazelcastInstance instance) {
        return getWanReplicationService(instance).getStats()
                                                 .get(WanCounterSyncTest.REPLICATION_NAME).getLocalWanPublisherStats()
                                                 .get(TARGET_GROUP_NAME).getOutboundQueueSize();
    }

    private WanBatchReplication getWanReplicationEndpoint(HazelcastInstance instance) {
        return (WanBatchReplication) getWanReplicationService(instance).getEndpoint(REPLICATION_NAME, TARGET_GROUP_NAME);
    }

}
