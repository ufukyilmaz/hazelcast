package com.hazelcast.internal.management.request;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.management.GetCacheEntryRequestTest;
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
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDGetCacheEntryRequestTest extends GetCacheEntryRequestTest {
    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{NATIVE}});
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Override
    protected CacheSimpleConfig getCacheConfig() {
        CacheSimpleConfig cacheConfig = super.getCacheConfig();
        cacheConfig.getEvictionConfig().setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
        return cacheConfig;
    }
}
