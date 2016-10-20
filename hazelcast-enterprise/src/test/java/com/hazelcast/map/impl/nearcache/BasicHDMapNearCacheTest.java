package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.getHDConfig;

/**
 * Basic HiDensity Near Cache tests for {@link com.hazelcast.core.IMap} on Hazelcast members.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class BasicHDMapNearCacheTest extends BasicMapNearCacheTest {

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
                {InMemoryFormat.NATIVE},
        });
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
