package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDIndexConfig;
import static com.hazelcast.spi.properties.ClusterProperty.GLOBAL_HD_INDEX_ENABLED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDLocalIndexStatsTest extends LocalIndexStatsTest {

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
        return getHDIndexConfig()
                .setProperty(GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
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
        assertEquals(1, stats().getQueryCount());
        assertEquals(1, stats().getIndexedQueryCount());
        assertEquals(1, valueStats().getQueryCount());
    }

}
