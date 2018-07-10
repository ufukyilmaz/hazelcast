package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.map.MapWanReplicationTestSupport;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;

/**
 * Test active-active replication of a map with 1 backup across two clusters of two nodes each. When map operations are only
 * executed on one cluster, no outgoing replication events should be created on the other cluster.
 * See https://hazelcast.zendesk.com/agent/tickets/1995
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiNodeWanReplicationTest extends MapWanReplicationTestSupport {

    private static final String MAP_NAME = "ZD1995";

    @Test
    public void testTwoClustersTwoNodesReplication() {
        final String atobWanReplicationName = "atob";
        final String btoaWanReplicationName = "btoa";
        final String clusterAGroupName = configA.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, atobWanReplicationName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, btoaWanReplicationName, PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();


        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        final AtomicBoolean testFailed = new AtomicBoolean();
        final StringBuilder failureMessageBuilder = new StringBuilder();

        // check outbound queue sizes every second
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (!testFailed.get()) {
                    LocalWanPublisherStats staging1Stats = getWanPublisherStats(
                            clusterB[0], btoaWanReplicationName, clusterAGroupName);
                    LocalWanPublisherStats staging2Stats = getWanPublisherStats(
                            clusterB[1], btoaWanReplicationName, clusterAGroupName);
                    if (staging1Stats.getTotalPublishedEventCount() > 0
                            || staging1Stats.getOutboundQueueSize() > 0
                            || staging2Stats.getTotalPublishedEventCount() > 0
                            || staging2Stats.getOutboundQueueSize() > 0) {
                        // fail the test
                        failureMessageBuilder.append(
                                format("No replication events should have been created on staging cluster nodes, however"
                                                + " there were %d published & %d queued events on staging1 and %d published"
                                                + " & %d queued events on staging2",
                                        staging1Stats.getTotalPublishedEventCount(),
                                        staging1Stats.getOutboundQueueSize(),
                                        staging2Stats.getTotalPublishedEventCount(),
                                        staging2Stats.getOutboundQueueSize()));
                        testFailed.set(true);
                    }
                }
            }
        }, 0, 100, MILLISECONDS);

        // add a new entry per second from each node on A cluster
        final IMap<String, String> mapOnA1 = clusterA[0].getMap(MAP_NAME);
        final IMap<String, String> mapOnA2 = clusterA[1].getMap(MAP_NAME);
        scheduler.scheduleAtFixedRate(new Runnable() {
            int counter = 0;

            @Override
            public void run() {
                mapOnA1.put("" + counter, "" + counter++);
                mapOnA2.put("" + counter, "" + counter++);
            }
        }, 0, 1, TimeUnit.SECONDS);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse(failureMessageBuilder.toString(), testFailed.get());
            }
        }, 30);
    }

    private LocalWanPublisherStats getWanPublisherStats(HazelcastInstance hz,
                                                        String replicationConfigName,
                                                        String targetGroupName) {
        WanReplicationPublisherDelegate replicationPublisher = (WanReplicationPublisherDelegate) getNodeEngineImpl(hz)
                .getWanReplicationService().getWanReplicationPublisher(replicationConfigName);
        return replicationPublisher.getStats().get(targetGroupName);
    }

    @Override
    protected WanPublisherConfig targetCluster(Config config, int count) {
        final WanPublisherConfig wanConfig = super.targetCluster(config, count)
                                                  .setQueueCapacity(100)
                                                  .setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION);
        final Map<String, Comparable> props = wanConfig.getProperties();
        props.put("batch.size", 10);
        props.put("batch.max.delay.millis", 100);
        return wanConfig;
    }

    @Override
    protected Config getConfig() {
        final Config config = super.getConfig();
        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat())
              .setBackupCount(1)
              .setTimeToLiveSeconds(900)
              .setMaxIdleSeconds(900)
              .setEvictionPolicy(EvictionPolicy.LRU);
        return config;
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return BINARY;
    }
}
