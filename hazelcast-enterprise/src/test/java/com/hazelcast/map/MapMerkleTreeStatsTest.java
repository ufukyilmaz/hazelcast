package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.wan.impl.merkletree.MerkleTree;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class, NightlyTest.class})
public class MapMerkleTreeStatsTest extends HazelcastTestSupport {

    private static final String MAP_NAME_WITH_MERKLE = "mapWithMerkleTree";
    private static final String MAP_NAME_WITHOUT_MERKLE = "mapWithoutMerkleTree";
    private static final int INSTANCE_COUNT = 1;

    private IMap<String, String> mapWithMerkle;
    private IMap<String, String> mapWithoutMerkle;
    private MetricsRegistry registry;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        Config config = getConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        registry = getNode(instance).nodeEngine.getMetricsRegistry();
        mapWithMerkle = instance.getMap(MAP_NAME_WITH_MERKLE);
        mapWithoutMerkle = instance.getMap(MAP_NAME_WITHOUT_MERKLE);
    }

    protected Config getConfig() {
        Config cfg = super.getConfig();
        cfg.getMetricsConfig()
           .setMetricsForDataStructuresEnabled(true);

        MapConfig mapWithMerkleConfig = new MapConfig(MAP_NAME_WITH_MERKLE);
        mapWithMerkleConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapWithMerkleConfig.getMerkleTreeConfig()
                           .setEnabled(true)
                           .setDepth(4);
        cfg.addMapConfig(mapWithMerkleConfig);

        MapConfig mapWithoutConfig = new MapConfig(MAP_NAME_WITHOUT_MERKLE);
        mapWithoutConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        cfg.addMapConfig(mapWithoutConfig);

        return cfg;
    }

    @Test
    public void testMapHeapCostIncludesMerkleTreeFootprint() {
        for (int i = 0; i < 10000; i++) {
            mapWithMerkle.put(Integer.toString(i), Integer.toString(i));
            mapWithoutMerkle.put(Integer.toString(i), Integer.toString(i));
        }

        long heapCostWithMerkle = mapWithMerkle.getLocalMapStats().getHeapCost();
        long merkleTreesCostWithMerkle = mapWithMerkle.getLocalMapStats().getMerkleTreesCost();
        long heapCostWithoutMerkle = mapWithoutMerkle.getLocalMapStats().getHeapCost();
        long merkleTreesCostWithoutMerkle = mapWithoutMerkle.getLocalMapStats().getMerkleTreesCost();
        long merkleFootprintWithMerkle = getMerkleTreesFootprint(mapWithMerkle);

        assertEquals(heapCostWithoutMerkle + merkleFootprintWithMerkle, heapCostWithMerkle);
        assertEquals(merkleFootprintWithMerkle, merkleTreesCostWithMerkle);
        assertEquals(0, merkleTreesCostWithoutMerkle);

        assertProbesEventuallyMatch(heapCostWithoutMerkle, merkleFootprintWithMerkle);
    }

    private void assertProbesEventuallyMatch(final long heapCostWithoutMerkle, final long merkleFootprintWithMerkle) {
        assertTrueEventually(() -> {
            ProbeCatcher probesWithMerkle = new ProbeCatcher(MAP_NAME_WITH_MERKLE);
            ProbeCatcher probesWithoutMerkle = new ProbeCatcher(MAP_NAME_WITHOUT_MERKLE);
            registry.collect(probesWithMerkle);
            registry.collect(probesWithoutMerkle);

            assertEquals(heapCostWithoutMerkle + merkleFootprintWithMerkle, probesWithMerkle.heapCost);
            assertEquals(merkleFootprintWithMerkle, probesWithMerkle.merkleTreesCost);

            assertEquals(heapCostWithoutMerkle, probesWithoutMerkle.heapCost);
            assertEquals(0, probesWithoutMerkle.merkleTreesCost);
        });
    }

    private MapServiceContext getMapServiceContext(MapProxyImpl map) {
        MapService mapService = (MapService) map.getService();
        return mapService.getMapServiceContext();
    }

    private MerkleTree[] getMerkleTrees(IMap<String, String> map) {
        MapServiceContext mapServiceContext = getMapServiceContext((MapProxyImpl) map);
        IPartition[] partitions = mapServiceContext.getNodeEngine().getPartitionService().getPartitions();
        MerkleTree[] merkleTrees = new MerkleTree[partitions.length];

        for (int i = 0; i < partitions.length; i++) {
            IPartition partition = partitions[i];
            int partitionId = partition.getPartitionId();
            EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) mapServiceContext
                    .getPartitionContainer(partitionId);
            merkleTrees[i] = partitionContainer.getMerkleTreeOrNull(map.getName());
        }
        return merkleTrees;
    }

    private long getMerkleTreesFootprint(IMap<String, String> map) {
        MerkleTree[] merkleTrees = getMerkleTrees(map);
        long merkleTreesFootprint = 0;
        for (MerkleTree merkleTree : merkleTrees) {
            merkleTreesFootprint += merkleTree.footprint();
        }

        return merkleTreesFootprint;
    }

    static class ProbeCatcher implements MetricsCollector {
        private final String pattern;
        private long heapCost;
        private long merkleTreesCost;

        ProbeCatcher(String mapName) {
            this.pattern = "[name=" + mapName + ",unit=count,metric=map.%s]";
        }

        @Override
        public void collectLong(String name, long value, Set<MetricTarget> excludedTargets) {
            if (name.equals(String.format(pattern, "heapCost"))) {
                heapCost = value;
            } else if (name.equals(String.format(pattern, "merkleTreesCost"))) {
                merkleTreesCost = value;
            }
        }

        @Override
        public void collectDouble(String name, double value, Set<MetricTarget> excludedTargets) {
        }

        @Override
        public void collectException(String name, Exception e, Set<MetricTarget> excludedTargets) {
        }

        @Override
        public void collectNoValue(String name, Set<MetricTarget> excludedTargets) {
        }
    }

}
