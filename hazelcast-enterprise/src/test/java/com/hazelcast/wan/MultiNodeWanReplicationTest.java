package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;

/**
 * Test active-active replication of a map with 1 backup across two clusters of two nodes each. When map operations are only
 * executed on one cluster, no outgoing replication events should be created on the other cluster.
 * See https://hazelcast.zendesk.com/agent/tickets/1995
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class MultiNodeWanReplicationTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "ZD1995";

    // let's assume we have clusters "dev" & "staging"
    // map operations are executed on cluster "dev"
    public static final String GROUP_DEV = "dev";
    public static final String GROUP_STAGING = "staging";

    public static final String WAN_REPLICATION_DEV_TO_STAGING = "DEV-STAGING";
    public static final String WAN_REPLICATION_STAGING_TO_DEV = "STAGING-DEV";

    HazelcastInstance dev1, dev2, staging1, staging2;

    @Before
    public void setup() {
        // dev cluster members on 9701 & 9702
        dev1 = Hazelcast.newHazelcastInstance(devClusterConfig(9701));
        dev2 = Hazelcast.newHazelcastInstance(devClusterConfig(9702));
        // staging cluster members on 10701 & 10702
        staging1 = Hazelcast.newHazelcastInstance(stagingClusterConfig(10701));
        staging2 = Hazelcast.newHazelcastInstance(stagingClusterConfig(10702));
        // ensure dev & staging clusters are formed properly
        assertClusterSizeEventually(2, dev1, 15);
        assertClusterSizeEventually(2, staging1, 15);
    }

    @After
    public void teardown() {
        dev1.shutdown();
        dev2.shutdown();
        staging1.shutdown();
        staging2.shutdown();
    }

    @Test
    public void testTwoClustersTwoNodesReplication()
            throws InterruptedException {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        final AtomicBoolean testFailed = new AtomicBoolean();
        final StringBuilder failureMessageBuilder = new StringBuilder();

        // check outbound queue sizes every second
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (!testFailed.get()) {
                    ILogger logger = getNode(dev1).getLogger(MultiNodeWanReplicationTest.class);
                    LocalWanPublisherStats staging1Stats = getWanPublisherStats(staging1, WAN_REPLICATION_STAGING_TO_DEV,
                            GROUP_DEV);
                    LocalWanPublisherStats staging2Stats = getWanPublisherStats(staging2, WAN_REPLICATION_STAGING_TO_DEV,
                            GROUP_DEV);
                    if (staging1Stats.getTotalPublishedEventCount() > 0 || staging1Stats.getOutboundQueueSize() > 0
                            || staging2Stats.getTotalPublishedEventCount() > 0 || staging2Stats.getOutboundQueueSize() > 0) {
                        // fail the test
                        failureMessageBuilder.append(
                                String.format("No replication events should have been created on staging cluster nodes, " +
                                        "however there were %d published & %d queued events on staging1 and %d published " +
                                        "& %d queued events on staging2", staging1Stats.getTotalPublishedEventCount(),
                                staging1Stats.getOutboundQueueSize(), staging2Stats.getTotalPublishedEventCount(),
                                staging2Stats.getOutboundQueueSize()));
                        testFailed.set(true);
                    }
                }
            }
        }, 0, 100, MILLISECONDS);

        // add a new entry per second from each node on DEV cluster
        final IMap mapOnDev1 = dev1.getMap(MAP_NAME);
        final IMap mapOnDev2 = dev2.getMap(MAP_NAME);
        scheduler.scheduleAtFixedRate(new Runnable() {
            int counter = 0;
            @Override
            public void run() {
                mapOnDev1.put(""+counter, "" + counter++);
                mapOnDev2.put(""+counter, "" + counter++);
            }
        }, 0, 1, TimeUnit.SECONDS);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(failureMessageBuilder.toString(), testFailed.get());
            }
        }, 30);
    }

    private LocalWanPublisherStats getWanPublisherStats(HazelcastInstance hz, String replicationConfigName,
                                                        String targetGroupName) {
        WanReplicationPublisherDelegate replicationPublisher = (WanReplicationPublisherDelegate) getNodeEngineImpl(hz)
                .getWanReplicationService().getWanReplicationPublisher(replicationConfigName);
        return replicationPublisher.getStats().get(targetGroupName);
    }

    private Config devClusterConfig(int port) {
        return clusterConfig(port, GROUP_DEV, GROUP_STAGING, WAN_REPLICATION_DEV_TO_STAGING,
                Arrays.asList(new String[] {"127.0.0.1:9701", "127.0.0.1:9702"}),
                Arrays.asList(new String[] {"127.0.0.1:10701", "127.0.0.1:10702"}));
    }

    private Config stagingClusterConfig(int port) {
        return clusterConfig(port, GROUP_STAGING, GROUP_DEV, WAN_REPLICATION_STAGING_TO_DEV,
                Arrays.asList(new String[] {"127.0.0.1:10701", "127.0.0.1:10702"}),
                Arrays.asList(new String[] {"127.0.0.1:9701", "127.0.0.1:9702"}));
    }

    private Config clusterConfig(int port, String groupName, String wanTargetClusterName, String wanReplicationName,
                                 List<String> groupClusterMembers, List<String> targetClusterMembers) {
        Config cfg = new Config();
        cfg.setGroupConfig(new GroupConfig().setName(groupName).setPassword(groupName));
        cfg.getNetworkConfig().setPort(port).setPortAutoIncrement(false).setPortCount(1);
        // disable multicast
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        // TCP joiner config
        cfg.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setMembers(groupClusterMembers);

        Map<String, Comparable> replicationProperties = new HashMap<String, Comparable>();
        replicationProperties.put("batch.size", 10);
        replicationProperties.put("batch.max.delay.millis", 100);
        replicationProperties.put("endpoints", listToString(targetClusterMembers));
        replicationProperties.put("group.password", wanTargetClusterName);

        cfg.addWanReplicationConfig(new WanReplicationConfig().setName(wanReplicationName).addWanPublisherConfig(
                new WanPublisherConfig().setGroupName(wanTargetClusterName).setQueueCapacity(100).setQueueFullBehavior(
                        WANQueueFullBehavior.THROW_EXCEPTION).setProperties(replicationProperties).
                                                setClassName("com.hazelcast.enterprise.wan.replication.WanBatchReplication")));

        cfg.addMapConfig(new MapConfig(MAP_NAME).setBackupCount(1).setInMemoryFormat(BINARY).setTimeToLiveSeconds(900).
                setMaxIdleSeconds(900).setEvictionPolicy(EvictionPolicy.LRU).setEvictionPercentage(25).
                                                        setWanReplicationRef(new WanReplicationRef(wanReplicationName,
                                                                "com.hazelcast.map.merge.PassThroughMergePolicy",
                                                                new ArrayList<String>(), true)));

        return cfg;
    }

    private String listToString(List<String> strings) {
        StringBuffer sb = new StringBuffer();
        boolean firstEntry = true;
        for (String s : strings) {
            if (firstEntry) {
                firstEntry = false;
            } else {
                sb.append(",");
            }
            sb.append(s);
        }
        return sb.toString();
    }
}
