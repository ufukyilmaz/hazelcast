package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDQueryCacheMapLoaderTest extends QueryCacheMapLoaderTest {

    @Parameterized.Parameters(name = "{0}, inMemoryFormat {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new DefaultMapLoader(), NATIVE},
                {new SlowMapLoader(1), NATIVE},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

}
