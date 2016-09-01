package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.map.HDTestSupport;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapHDNearCacheTest extends ClientMapNearCacheTest {

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = super.newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE);
        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return nearCacheConfig;
    }

    @Override
    protected ClientConfig newClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(32, MemoryUnit.MEGABYTES))
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        ClientConfig clientConfig = super.newClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE);
        clientConfig.setNativeMemoryConfig(nativeMemoryConfig);

        return clientConfig;
    }

    @Override
    protected Config newConfig() {
        Config config = HDTestSupport.getHDConfig(super.newConfig());
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE);
        config.getMapConfig("default").setInMemoryFormat(NATIVE);
        return config;
    }

    @Override
    protected NearCacheConfig newNearCacheConfigWithEntryCountEviction(EvictionPolicy evictionPolicy, int size) {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();

        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(size);

        return nearCacheConfig;
    }

    /**
     * The EE Near Cache evicts a single entry per eviction.
     */
    @Override
    protected int getExpectedEvictionCount(int size) {
        return 1;
    }

    /**
     * HD backed near cache does not support NONE eviction policy.
     */
    @Test(expected = IllegalArgumentException.class)
    @Override
    public void testNearCacheInvalidation_WithNone_whenMaxSizeExceeded() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);
        getNearCachedMapFromClient(nearCacheConfig);
    }

    @Test
    @Override
    public void testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException() {
        // this test expects an IllegalArgumentException in OS, but should not throw any exception in EE
        super.testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException();
    }

    /**
     * HD backed near cache does not support RANDOM eviction policy.
     */
    @Test(expected = IllegalArgumentException.class)
    @Override
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.RANDOM);
        getNearCachedMapFromClient(nearCacheConfig);
    }

    @Override
    protected void assertNearCacheInvalidation_whenMaxSizeExceeded(NearCacheConfig config) {
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(config);
        populateNearCache(map, MAX_CACHE_SIZE);

        triggerEviction(map);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }
}
