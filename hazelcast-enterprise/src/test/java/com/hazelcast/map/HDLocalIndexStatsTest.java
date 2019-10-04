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

import static com.hazelcast.HDTestSupport.getHDConfig;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDLocalIndexStatsTest extends LocalIndexStatsTest {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.NATIVE}});
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @SuppressWarnings("unchecked")
    @Test
    @Override
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
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
