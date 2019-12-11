package com.hazelcast.wan;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Test that asserts that WAN sync events are not counted in the number of
 * WAN events from the WAN staging queue.
 * We must set the batch size to 1 since a lot of the events might be
 * drained from the staging queue before we get to the assertion.
 * By setting the batch size to 1, we fill up the executor job queue as well
 * as fill up the staging queue. Since now all queues are full and some
 * events are still pending to be enqueued to the staging queue, we must
 * periodically clear the WAN queues until all events have been cleared.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanCounterSyncClearTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private ScheduledExecutorService executorService;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @After
    public void cleanup() {
        executorService.shutdown();
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 1).setup();
        targetCluster = clusterB(factory, 1).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withReplicationBatchSize(1)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Test
    public void syncEventsDoNotCountAgainstWanQueueSize() {
        sourceCluster.startCluster();

        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);
        fillMap(sourceCluster, MAP_NAME, 0, 1000);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);

        sourceCluster.syncMap(wanReplication, MAP_NAME);

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                sourceCluster.clearWanQueuesOnAllMembers(wanReplication);
            }
        }, 5, 5, SECONDS);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                verifyEventCountersAreZero(sourceCluster, wanReplication);
            }
        }, 30);
    }
}
