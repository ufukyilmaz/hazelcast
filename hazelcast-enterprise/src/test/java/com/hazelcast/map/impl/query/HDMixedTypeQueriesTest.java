package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getSmallInstanceHDIndexConfig;
import static com.hazelcast.query.impl.HDGlobalIndexProvider.PROPERTY_GLOBAL_HD_INDEX_ENABLED;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMixedTypeQueriesTest extends MixedTypeQueriesTest {

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
        config.setProperty(PROPERTY_GLOBAL_HD_INDEX_ENABLED.getName(), globalIndex);
        return config;
    }

}
