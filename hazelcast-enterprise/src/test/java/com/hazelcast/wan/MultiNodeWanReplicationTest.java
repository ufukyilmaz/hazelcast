package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.wan.impl.DelegatingWanReplicationScheme;
import com.hazelcast.wan.map.MapWanReplicationTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;

/**
 * Test active-active replication of a map with 1 backup across two clusters of two nodes each. When map operations are only
 * executed on one cluster, no outgoing replication events should be created on the other cluster.
 * See https://hazelcast.zendesk.com/agent/tickets/1995
 * See https://hazelcast.zendesk.com/agent/tickets/4195
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiNodeWanReplicationTest extends MapWanReplicationTestSupport {

    private static final String MAP_NAME = "ZD1995";

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.NATIVE}
        });
    }

    @Test
    public void testTwoClustersTwoNodesReplication() {
        final String atobWanReplicationName = "atob";
        final String btoaWanReplicationName = "btoa";
        final String clusterAName = configA.getClusterName();
        setupReplicateFrom(configA, configB, clusterB.length, atobWanReplicationName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, btoaWanReplicationName, PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        final AtomicBoolean testFailed = new AtomicBoolean();
        final StringBuilder failureMessageBuilder = new StringBuilder();

        // check outbound queue sizes and event counters at every 100 millis
        startVerifyingStagingWanStats(btoaWanReplicationName, clusterAName, scheduler, testFailed, failureMessageBuilder);

        // add a new entry per second from each node on A cluster
        final IMap<String, String> mapOnA1 = clusterA[0].getMap(MAP_NAME);
        final IMap<String, String> mapOnA2 = clusterA[1].getMap(MAP_NAME);

        startPuttingInClusterA(scheduler, mapOnA1, mapOnA2);
        startRemovingFromClusterAWithDelay(scheduler, mapOnA1, mapOnA2, 5);

        assertTrueAllTheTime(() -> assertFalse(failureMessageBuilder.toString(), testFailed.get()), 30);
    }

    private void startPuttingInClusterA(ScheduledExecutorService scheduler, final IMap<String, String> mapOnA1,
                                        final IMap<String, String> mapOnA2) {
        scheduler.scheduleAtFixedRate(new Runnable() {
            int counter = 0;

            @Override
            public void run() {
                mapOnA1.put("" + counter, "" + counter++);
                mapOnA2.put("" + counter, "" + counter++);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void startRemovingFromClusterAWithDelay(ScheduledExecutorService scheduler, final IMap<String, String> mapOnA1,
                                                    final IMap<String, String> mapOnA2, int delaySecs) {
        scheduler.scheduleAtFixedRate(new Runnable() {
            int counter = 0;

            @Override
            public void run() {
                mapOnA1.remove("" + counter++);
                mapOnA2.remove("" + counter++);
            }
        }, delaySecs, 1, TimeUnit.SECONDS);
    }

    private void startVerifyingStagingWanStats(final String btoaWanReplicationName, final String clusterAName,
                                               ScheduledExecutorService scheduler, final AtomicBoolean testFailed,
                                               final StringBuilder failureMessageBuilder) {
        scheduler.scheduleAtFixedRate(() -> {
            if (!testFailed.get()) {
                LocalWanPublisherStats staging1Stats = getWanPublisherStats(
                        clusterB[0], btoaWanReplicationName, clusterAName);
                LocalWanPublisherStats staging2Stats = getWanPublisherStats(
                        clusterB[1], btoaWanReplicationName, clusterAName);
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
        }, 0, 100, MILLISECONDS);
    }

    private LocalWanPublisherStats getWanPublisherStats(HazelcastInstance hz,
                                                        String replicationConfigName,
                                                        String targetClusterName) {
        DelegatingWanReplicationScheme replicationPublisher =
                getNodeEngineImpl(hz).getWanReplicationService()
                                     .getWanReplicationPublishers(replicationConfigName);
        return replicationPublisher.getStats().get(targetClusterName);
    }

    @Override

    protected WanBatchReplicationPublisherConfig targetCluster(Config config, int count) {
        final WanBatchReplicationPublisherConfig wanConfig =
                super.targetCluster(config, count)
                     .setQueueCapacity(100)
                     .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION)
                     .setBatchSize(10)
                     .setBatchMaxDelayMillis(100);
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
    public InMemoryFormat getMemoryFormat() {
        return inMemoryFormat;
    }
}
