package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Caching;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_DISCRIMINATOR_REPLICATION;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_ACK_DELAY_CURRENT_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_ACK_DELAY_LAST_END;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_ACK_DELAY_LAST_START;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_ACK_DELAY_TOTAL_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_ACK_DELAY_TOTAL_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_CONSISTENCY_CHECK_LAST_DIFF_PARTITION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_AVG_ENTRIES_PER_LEAF;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_MERKLE_SYNC_PARTITIONS_SYNCED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_OUTBOUND_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_UPDATE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_PREFIX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_PREFIX_CONSISTENCY_CHECK;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_PREFIX_SYNC;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_TAG_CACHE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_TAG_MAP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_TAG_PUBLISHERID;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.spi.properties.ClusterProperty.WAN_CONSUMER_INVOCATION_THRESHOLD;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseWanMetricsTest extends HazelcastTestSupport {
    private static final String CACHE_NAME = "myCache";
    private static final String MAP_NAME = "myMap";
    private static final String REPLICATION_NAME = "wanReplication";

    @Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
            {NONE},
            {MERKLE_TREES}
        });
    }

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @BeforeClass
    public static void setupClass() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupClass() {
        JsrTestUtil.cleanup();
    }

    @After
    public void cleanup() {
        Caching.getCachingProvider().close();
        factory.shutdownAll();
        System.clearProperty(WAN_CONSUMER_INVOCATION_THRESHOLD.getName());
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 1).setup();
        targetCluster = clusterB(factory, 1).setup();

        configureCache(sourceCluster);
        configureCache(targetCluster);

        configureMerkleTrees(sourceCluster);
        configureMerkleTrees(targetCluster);

        wanReplication = replicate()
            .from(sourceCluster)
            .to(targetCluster)
            .withConsistencyCheckStrategy(consistencyCheckStrategy)
            .withSetupName(REPLICATION_NAME)
            .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();
    }

    private void configureMerkleTrees(Cluster cluster) {
        if (consistencyCheckStrategy == MERKLE_TREES) {
            cluster.getConfig().getMapConfig("default")
                   .getMerkleTreeConfig()
                   .setEnabled(true)
                   .setDepth(6);
        }
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig()
            .setMaxSizePolicy(ENTRY_COUNT);

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig).getMergePolicyConfig()
                   .setPolicy(PassThroughMergePolicy.class.getName());
    }

    @Test
    public void testReplicationMetrics() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 100);
        fillCache(sourceCluster, CACHE_NAME, 0, 100);

        MetricDescriptor descriptorOutboundQ = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_OUTBOUND_QUEUE_SIZE)
                .withUnit(COUNT)
                .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, REPLICATION_NAME)
                .withTag(WAN_TAG_PUBLISHERID, targetCluster.getName());

        MetricDescriptor descriptorMap = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_UPDATE_COUNT)
                .withUnit(COUNT)
                .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, REPLICATION_NAME)
                .withTag(WAN_TAG_PUBLISHERID, targetCluster.getName())
                .withTag(WAN_TAG_MAP, MAP_NAME);

        MetricDescriptor descriptorCache = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_UPDATE_COUNT)
                .withUnit(COUNT)
                .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, REPLICATION_NAME)
                .withTag(WAN_TAG_PUBLISHERID, targetCluster.getName())
                .withTag(WAN_TAG_CACHE, CACHE_NAME);

        assertHasStatsEventually(sourceCluster, descriptorOutboundQ, descriptorMap, descriptorCache);
    }

    @Test
    public void testSyncMetrics() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, 100);

        sourceCluster.syncMap(wanReplication, MAP_NAME);

        MetricDescriptor descriptorPartitionsSynced = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX_SYNC)
                .withMetric(WAN_METRIC_MERKLE_SYNC_PARTITIONS_SYNCED)
                .withUnit(COUNT)
                .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, REPLICATION_NAME)
                .withTag(WAN_TAG_PUBLISHERID, targetCluster.getName())
                .withTag(WAN_TAG_MAP, MAP_NAME);

        if (MERKLE_TREES == consistencyCheckStrategy) {
            MetricDescriptor descriptorAvgEntriesPerLeaf = DEFAULT_DESCRIPTOR_SUPPLIER
                    .get()
                    .withPrefix(WAN_PREFIX_SYNC)
                    .withMetric(WAN_METRIC_MERKLE_SYNC_AVG_ENTRIES_PER_LEAF)
                    .withUnit(COUNT)
                    .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, REPLICATION_NAME)
                    .withTag(WAN_TAG_PUBLISHERID, targetCluster.getName())
                    .withTag(WAN_TAG_MAP, MAP_NAME);

            MetricDescriptor descriptorConsistencyCheck = DEFAULT_DESCRIPTOR_SUPPLIER
                    .get()
                    .withPrefix(WAN_PREFIX_CONSISTENCY_CHECK)
                    .withMetric(WAN_METRIC_CONSISTENCY_CHECK_LAST_DIFF_PARTITION_COUNT)
                    .withUnit(COUNT)
                    .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, REPLICATION_NAME)
                    .withTag(WAN_TAG_PUBLISHERID, targetCluster.getName())
                    .withTag(WAN_TAG_MAP, MAP_NAME);

            assertHasStatsEventually(sourceCluster, descriptorPartitionsSynced, descriptorAvgEntriesPerLeaf,
                    descriptorConsistencyCheck);
        } else {
            assertHasStatsEventually(sourceCluster, descriptorPartitionsSynced);
        }
    }

    @Test
    public void testAcknowledgerMetricsIfDelay() {
        System.setProperty(WAN_CONSUMER_INVOCATION_THRESHOLD.getName(), Integer.toString(1));
        sourceCluster.startCluster();
        targetCluster.startCluster();

        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, 10000);

        sourceCluster.syncMap(wanReplication, MAP_NAME);

        MetricDescriptor descriptorTotalCount = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_TOTAL_COUNT)
                .withUnit(COUNT);

        MetricDescriptor descriptorTotalMillis = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_TOTAL_MILLIS)
                .withUnit(MS);

        MetricDescriptor descriptorLastStart = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_LAST_START)
                .withUnit(MS);

        MetricDescriptor descriptorLastEnd = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_LAST_END)
                .withUnit(MS);

        MetricDescriptor descriptorCurrentMillis = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_CURRENT_MILLIS)
                .withUnit(MS);

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        assertHasStatsEventually(targetCluster, collector -> {
            Map<MetricDescriptor, CapturingCollector.Capture> captures = collector.captures();
            if (consistencyCheckStrategy == NONE) {
                // NONE: a batch in this test contains typically 500 entries -> 10000 entries/500 batch size = 20 ACK delays
                // it's possible though that the synchronization queue is not full when the publisher collects the next batch
                // in this case it sends a smaller batch that results in more than 20 batches, hence more than 20 ACK delays
                return captures.get(descriptorTotalCount).singleCapturedValue().intValue() >= 20
                        && captures.get(descriptorTotalMillis).singleCapturedValue().longValue() > 0
                        && captures.get(descriptorLastStart).singleCapturedValue().longValue() > 0
                        && captures.get(descriptorLastEnd).singleCapturedValue().longValue() > 0
                        && captures.get(descriptorCurrentMillis).singleCapturedValue().longValue() == -1;
            } else {
                // MERKLE_TREE: a batch contains MT leaves with multiple entries
                // in this test we sync the 10000 entries only in one or a few batches
                return captures.get(descriptorTotalCount).singleCapturedValue().intValue() > 0
                        && captures.get(descriptorTotalMillis).singleCapturedValue().longValue() > 0
                        && captures.get(descriptorLastStart).singleCapturedValue().longValue() > 0
                        && captures.get(descriptorLastEnd).singleCapturedValue().longValue() > 0
                        && captures.get(descriptorCurrentMillis).singleCapturedValue().longValue() == -1;
            }
        });
    }

    @Test
    public void testAcknowledgerMetricsIfNoDelay() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, 10000);

        sourceCluster.syncMap(wanReplication, MAP_NAME);

        MetricDescriptor descriptorTotalCount = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_TOTAL_COUNT)
                .withUnit(COUNT);

        MetricDescriptor descriptorTotalMillis = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_TOTAL_MILLIS)
                .withUnit(MS);

        MetricDescriptor descriptorLastStart = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_LAST_START)
                .withUnit(MS);

        MetricDescriptor descriptorLastEnd = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_LAST_END)
                .withUnit(MS);

        MetricDescriptor descriptorCurrentMillis = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(WAN_PREFIX)
                .withMetric(WAN_METRIC_ACK_DELAY_CURRENT_MILLIS)
                .withUnit(MS);

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        assertHasStatsEventually(targetCluster, collector -> {
            Map<MetricDescriptor, CapturingCollector.Capture> captures = collector.captures();
            return captures.get(descriptorTotalCount).singleCapturedValue().intValue() == 0
                    && captures.get(descriptorTotalMillis).singleCapturedValue().longValue() == 0
                    && captures.get(descriptorLastStart).singleCapturedValue().longValue() == 0
                    && captures.get(descriptorLastEnd).singleCapturedValue().longValue() == 0
                    && captures.get(descriptorCurrentMillis).singleCapturedValue().longValue() == -1;
        });
    }

    private void assertHasStatsEventually(Cluster cluster, MetricDescriptor... expectedDescriptors) {
        assertHasStatsEventually(cluster, collector -> {
            for (MetricDescriptor expectedDescriptor : expectedDescriptors) {
                return collector.captures().containsKey(expectedDescriptor);
            }
            return false;
        });
    }

    private void assertHasStatsEventually(Cluster cluster, Function<CapturingCollector, Boolean> assertFn) {
        assertTrueEventually(() -> {
            boolean found = true;
            for (HazelcastInstance member : cluster.getMembers()) {
                MetricsRegistry metricsRegistry = getNodeEngineImpl(member).getMetricsRegistry();
                CapturingCollector collector = new CapturingCollector();
                metricsRegistry.collect(collector);

                found &= assertFn.apply(collector);
            }
            assertTrue(found);
        });
    }

}
