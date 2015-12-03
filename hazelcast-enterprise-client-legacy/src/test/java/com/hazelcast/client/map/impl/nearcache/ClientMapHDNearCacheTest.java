package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
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
        NativeMemoryConfig nativeMemoryConfig = newNativeMemoryConfig();
        clientConfig.setNativeMemoryConfig(nativeMemoryConfig);
        return clientConfig;
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

}
