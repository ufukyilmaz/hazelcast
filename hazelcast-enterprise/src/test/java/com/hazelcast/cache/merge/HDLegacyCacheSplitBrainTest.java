package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDLegacyCacheSplitBrainTest extends LegacyCacheSplitBrainTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameters(name = "inMemoryFormat:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, CustomCacheMergePolicy.class},
                {OBJECT, CustomCacheMergePolicy.class},
        });
    }

    @Override
    protected Config config() {
        return getHDConfig(super.config(), POOLED, MEMORY_SIZE);
    }

    @Override
    protected CacheConfig newCacheConfig(String cacheName, Class<? extends CacheMergePolicy> mergePolicy,
                                         InMemoryFormat inMemoryFormat) {
        CacheConfig config = super.newCacheConfig(cacheName, mergePolicy, inMemoryFormat);
        if (inMemoryFormat == NATIVE) {
            config.getEvictionConfig().setMaximumSizePolicy(USED_NATIVE_MEMORY_SIZE);
        }
        return config;
    }
}
