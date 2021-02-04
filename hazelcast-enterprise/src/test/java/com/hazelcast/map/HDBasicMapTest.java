package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.test.annotation.ConfigureParallelRunnerWith;
import com.hazelcast.test.annotation.HeavilyMultiThreadedTestLimiter;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.HDTestSupport.getHDIndexConfig;
import static java.util.Arrays.asList;

/**
 * Basic map tests for HD-IMap.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@ConfigureParallelRunnerWith(HeavilyMultiThreadedTestLimiter.class)
public class HDBasicMapTest extends BasicMapTest {

    @Parameterized.Parameter
    public boolean statisticsEnabled;

    @Parameterized.Parameters(name = "statisticsEnabled:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    @Override
    protected Config getConfig() {
        MapConfig ttlMapConfig = new MapConfig("mapWithTTL*");
        ttlMapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        ttlMapConfig.setTimeToLiveSeconds(1);
        ttlMapConfig.setStatisticsEnabled(statisticsEnabled);
        getHDConfig().addMapConfig(ttlMapConfig);
        return getHDIndexConfig(super.getConfig()).addMapConfig(ttlMapConfig);
    }
}
