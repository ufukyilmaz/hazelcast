package com.hazelcast.internal.eviction;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.NativeMemoryTestUtil.assertFreeNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class HDCacheExpirationStressTest extends CacheExpirationStressTest {

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @Override
    protected Config getConfig() {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES));

        return super.getConfig()
                .setNativeMemoryConfig(memoryConfig)
                .setProperty(PARTITION_COUNT.getName(), "5");
    }

    @Override
    protected CacheConfig getCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);
        return super.getCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected void assertRecords(HazelcastInstance[] instances) {
        super.assertRecords(instances);
        assertFreeNativeMemory(instances);
    }
}
