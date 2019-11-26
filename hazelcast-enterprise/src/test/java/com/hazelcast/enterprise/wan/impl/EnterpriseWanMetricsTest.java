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
import org.apache.commons.lang.mutable.MutableBoolean;
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

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
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
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

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
            .withPrefix("wan")
            .withMetric("outboundQueueSize")
            .withUnit(COUNT)
            .withDiscriminator("replication", REPLICATION_NAME)
            .withTag("publisherId", targetCluster.getName());

        MetricDescriptor descriptorMap = DEFAULT_DESCRIPTOR_SUPPLIER
            .get()
            .withPrefix("wan")
            .withMetric("updateCount")
            .withUnit(COUNT)
            .withDiscriminator("replication", REPLICATION_NAME)
            .withTag("publisherId", targetCluster.getName())
            .withTag("map", MAP_NAME);

        MetricDescriptor descriptorCache = DEFAULT_DESCRIPTOR_SUPPLIER
            .get()
            .withPrefix("wan")
            .withMetric("updateCount")
            .withUnit(COUNT)
            .withDiscriminator("replication", REPLICATION_NAME)
            .withTag("publisherId", targetCluster.getName())
            .withTag("cache", CACHE_NAME);

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
            .withPrefix("wan.sync")
            .withMetric("partitionsSynced")
            .withUnit(COUNT)
            .withDiscriminator("replication", REPLICATION_NAME)
            .withTag("publisherId", targetCluster.getName())
            .withTag("map", MAP_NAME);

        if (MERKLE_TREES == consistencyCheckStrategy) {
            MetricDescriptor descriptorAvgEntriesPerLeaf = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("wan.sync")
                .withMetric("avgEntriesPerLeaf")
                .withUnit(COUNT)
                .withDiscriminator("replication", REPLICATION_NAME)
                .withTag("publisherId", targetCluster.getName())
                .withTag("map", MAP_NAME);

            MetricDescriptor descriptorConsistencyCheck = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("wan.consistencyCheck")
                .withMetric("lastDiffPartitionCount")
                .withUnit(COUNT)
                .withDiscriminator("replication", REPLICATION_NAME)
                .withTag("publisherId", targetCluster.getName())
                .withTag("map", MAP_NAME);

            assertHasStatsEventually(sourceCluster, descriptorPartitionsSynced, descriptorAvgEntriesPerLeaf,
                descriptorConsistencyCheck);
        } else {
            assertHasStatsEventually(sourceCluster, descriptorPartitionsSynced);
        }
    }

    private void assertHasStatsEventually(Cluster cluster, MetricDescriptor... expectedDescriptors) {
        assertTrueEventually(() -> {
            MutableBoolean found = new MutableBoolean(true);
            for (HazelcastInstance member : cluster.getMembers()) {
                MetricsRegistry metricsRegistry = getNodeEngineImpl(member).getMetricsRegistry();
                CapturingCollector collector = new CapturingCollector();
                metricsRegistry.collect(collector);

                for (MetricDescriptor expectedDescriptor : expectedDescriptors) {
                    found.setValue(found.booleanValue() && collector.captures().containsKey(expectedDescriptor));
                }
            }
            assertTrue(found.booleanValue());
        });
    }

}
