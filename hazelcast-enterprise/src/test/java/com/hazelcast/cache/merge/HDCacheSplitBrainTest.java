package com.hazelcast.cache.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.AssertTask;
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
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDCacheSplitBrainTest extends CacheSplitBrainTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE, DiscardMergePolicy.class},
                {NATIVE, HigherHitsMergePolicy.class},
                {NATIVE, LatestAccessMergePolicy.class},
                {NATIVE, PassThroughMergePolicy.class},
                {NATIVE, PutIfAbsentMergePolicy.class},

                {BINARY, MergeIntegerValuesMergePolicy.class},
                {OBJECT, MergeIntegerValuesMergePolicy.class},
                {NATIVE, MergeIntegerValuesMergePolicy.class},
        });
    }

    @Override
    protected Config config() {
        EvictionConfig evictionConfig = new EvictionConfig();
        if (inMemoryFormat == NATIVE) {
            evictionConfig.setMaximumSizePolicy(USED_NATIVE_MEMORY_SIZE);
        }

        Config config = getHDConfig(super.config(), POOLED, MEMORY_SIZE);
        config.getCacheConfig(cacheNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setEvictionConfig(evictionConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(true)
                .setMergePolicy(mergePolicyClass.getName());
        config.getCacheConfig(cacheNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setEvictionConfig(evictionConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(true)
                .setMergePolicy(mergePolicyClass.getName());
        return config;
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HDCacheSplitBrainTest.super.onAfterSplitBrainHealed(instances);
            }
        }, 30);
    }
}
