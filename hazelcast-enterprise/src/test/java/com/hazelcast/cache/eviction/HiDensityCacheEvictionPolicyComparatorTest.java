package com.hazelcast.cache.eviction;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.QuickMath;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HiDensityCacheEvictionPolicyComparatorTest extends AbstractCacheEvictionPolicyComparatorTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(4, MemoryUnit.MEGABYTES);
    private static final int ITERATION_COUNT
            = (int) (MEMORY_SIZE.bytes() / (16 + 16 + QuickMath.nextPowerOfTwo(HiDensityNativeMemoryCacheRecord.SIZE)));

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        return instanceFactory.newHazelcastInstance(config);
    }

    @Override
    protected ConcurrentMap getUserContext(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getUserContext();
    }

    @Override
    protected Config createConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setMetadataSpacePercentage(50f)
                .setPageSize((int) (MEMORY_SIZE.bytes() / 8))
                .setSize(MEMORY_SIZE);

        return super.createConfig()
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "4")
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected CacheConfig<Integer, String> createCacheConfig(String cacheName) {
        return super.createCacheConfig(cacheName)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorClassName_when_maxSizePolicy_is_usedNativeMemoryPercentage() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(50)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setComparatorClassName(MyEvictionPolicyComparator.class.getName());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_usedNativeMemoryPercentage() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(50)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setComparator(new MyEvictionPolicyComparator());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorClassName_when_maxSizePolicy_is_freeNativeMemoryPercentage() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(50)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE)
                .setComparatorClassName(MyEvictionPolicyComparator.class.getName());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_freeNativeMemoryPercentage() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(50)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE)
                .setComparator(new MyEvictionPolicyComparator());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorClassName_when_maxSizePolicy_is_usedNativeMemorySize() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize((int) (MEMORY_SIZE.megaBytes() / 4))
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)
                .setComparatorClassName(MyEvictionPolicyComparator.class.getName());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_usedNativeMemorySize() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize((int) (MEMORY_SIZE.megaBytes() / 4))
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)
                .setComparator(new MyEvictionPolicyComparator());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorClassName_when_maxSizePolicy_is_freeNativeMemorySize() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize((int) (MEMORY_SIZE.megaBytes() / 4))
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE)
                .setComparatorClassName(MyEvictionPolicyComparator.class.getName());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_freeNativeMemorySize() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize((int) (MEMORY_SIZE.megaBytes() / 4))
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE)
                .setComparator(new MyEvictionPolicyComparator());

        testEvictionPolicyComparator(evictionConfig, ITERATION_COUNT);
    }
}
