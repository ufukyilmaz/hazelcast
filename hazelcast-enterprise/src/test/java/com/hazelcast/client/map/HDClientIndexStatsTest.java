package com.hazelcast.client.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDIndexConfig;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({ QuickTest.class })
public class HDClientIndexStatsTest extends ClientIndexStatsTest {

    @Parameterized.Parameter(1)
    public String globalIndex;

    @Parameterized.Parameters(name = "format:{0} globalIndex:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, "true"},
                {InMemoryFormat.NATIVE, "false"},
        });
    }

    @Override
    protected Config getConfig() {
        Config config = getSmallInstanceHDIndexConfig();
        config.setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
        return config;
    }

    @SuppressWarnings("unchecked")
    @Test
    @Override
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
        assumeTrue(globalIndex.equals("false"));
        addIndex(map, "this", false);

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        map.entrySet(Predicates.partitionPredicate(10, Predicates.equal("this", 10)));
        LocalMapStats stats1 = map1.getLocalMapStats();
        LocalMapStats stats2 = map2.getLocalMapStats();
        assertTrue(stats1.getQueryCount() == 1 && stats2.getQueryCount() == 0
                || stats1.getQueryCount() == 0 && stats2.getQueryCount() == 1);
        assertTrue(stats1.getIndexedQueryCount() == 1 && stats2.getIndexedQueryCount() == 0
                || stats1.getIndexedQueryCount() == 0 && stats2.getIndexedQueryCount() == 1);
        LocalIndexStats indexStats1 = stats1.getIndexStats().get("this");
        LocalIndexStats indexStats2 = stats2.getIndexStats().get("this");
        assertTrue(indexStats1.getQueryCount() == 1 && indexStats2.getQueryCount() == 0
                || indexStats1.getQueryCount() == 0 && indexStats2.getQueryCount() == 1);
    }

}
