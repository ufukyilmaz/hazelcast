package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapHDNearCacheTest extends ClientMapNearCacheTest {

    @BeforeClass
    public static void setup() throws Exception {
        MAX_CACHE_SIZE = 50000;
    }

    protected static NativeMemoryConfig newNativeMemoryConfig() {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig();
        memoryConfig.setEnabled(true);
        memoryConfig.setSize(new MemorySize(32, MemoryUnit.MEGABYTES));
        memoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        return memoryConfig;
    }

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = super.newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE);

        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
        evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy()));
        evictionConfig.setSize(1);

        return nearCacheConfig;
    }

    @Override
    protected ClientConfig newClientConfig() {
        ClientConfig clientConfig = super.newClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE);
        NativeMemoryConfig nativeMemoryConfig = newNativeMemoryConfig();
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

    /**
     * HD backed near cache does not support NONE eviction policy.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheInvalidation_WithNone_whenMaxSizeExceeded() throws Exception {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);
        getNearCachedMapFromClient(nearCacheConfig);
    }

    /**
     * HD backed near cache does not support RANDOM eviction policy.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() throws Exception {
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
