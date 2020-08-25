package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.impl.HDGlobalIndexImpl;
import com.hazelcast.query.impl.HDIndexImpl;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.predicates.BoundedRangePredicate;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDConfig;
import static com.hazelcast.HDTestSupport.getSmallInstanceHDIndexConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static com.hazelcast.query.impl.predicates.BoundedRangePredicateQueriesTest.Person;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDGlobalVersusPartitionedIndexTest extends HazelcastTestSupport {

    @Parameterized.Parameter(0)
    public String globalIndex;

    @Parameterized.Parameters(name = "globalIndex:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"true"},
                {"false"},
        });
    }

    @Test
    public void testGlobalVsPartitionedIndex() {
        boolean isGlobalIndex = Boolean.valueOf(globalIndex);
        Config config = getConfig();
        String mapName = "persons";
        config.getMapConfig(mapName).setInMemoryFormat(NATIVE);
        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<Integer, Person> map = hz.getMap(mapName);
        map.addIndex(IndexType.SORTED, "age");

        for (int i = 0; i < 10; ++i) {
            map.put(i, new Person(i));
        }

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);

        int partitionId = nodeEngine.getPartitionService().getPartitions()[0].getPartitionId();

        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        assertEquals(isGlobalIndex, mapServiceContext.globalIndexEnabled());

        // Check there is only one global index for all partitions
        Indexes prevIndexes = null;
        for (IPartition partition : nodeEngine.getPartitionService().getPartitions()) {
            Indexes indexes = mapServiceContext.getMapContainer(mapName).getIndexes(partition.getPartitionId());
            if (prevIndexes == null) {
                prevIndexes = indexes;
            } else {
                if (isGlobalIndex) {
                    assertTrue(prevIndexes == indexes);
                } else {
                    assertTrue(prevIndexes != indexes);
                }
            }
        }

        Indexes indexes = mapServiceContext.getMapContainer(mapName).getIndexes(partitionId);

        assertEquals(isGlobalIndex, indexes.isGlobal());
        InternalIndex[] internalIndexes = indexes.getIndexes();
        assertEquals(1, internalIndexes.length);
        if (isGlobalIndex) {
            assertTrue(internalIndexes[0] instanceof HDGlobalIndexImpl);
        } else {
            assertTrue(internalIndexes[0] instanceof HDIndexImpl);
        }

        InternalIndex index = internalIndexes[0];
        assertEquals(0, index.getPerIndexStats().getHitCount());

        // Run a query using the index
        BoundedRangePredicate predicate = new BoundedRangePredicate("age", 0, true, 10, false);
        Set<Map.Entry<Integer, Person>> result = map.entrySet(predicate);
        assertEquals(10, result.size());

        // Check the query is executed using one range scan
        assertEquals(1, index.getPerIndexStats().getQueryCount());
    }

    @Test
    public void testNotPooledMemoryManager() {
        assumeTrue(Boolean.valueOf(globalIndex));
        Config config = getSmallInstanceHDConfig();
        config.getNativeMemoryConfig().setAllocatorType(STANDARD);
        String mapName = "persons";
        config.getMapConfig(mapName).setInMemoryFormat(NATIVE);
        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<Integer, Person> map = hz.getMap(mapName);

        assertThrows(IllegalArgumentException.class, () -> map.addIndex(IndexType.SORTED, "age"));
    }

    @Override
    protected Config getConfig() {
        Config config = getSmallInstanceHDIndexConfig();
        config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
        return config;
    }
}
