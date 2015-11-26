package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapHDNearCacheTest extends ClientMapNearCacheTest {

    @BeforeClass
    public static void setup() throws Exception {
        MAX_CACHE_SIZE = 50000;

        ClientMapNearCacheTest.setup();

        Map<String, NearCacheConfig> nearCacheConfigs = clientConfig.getNearCacheConfigMap();
        Collection<NearCacheConfig> values = nearCacheConfigs.values();
        for (NearCacheConfig nearCacheConfig : values) {
            nativize(nearCacheConfig);
        }

        NativeMemoryConfig memoryConfig = newNativeMemoryConfig();

        clientConfig.setNativeMemoryConfig(memoryConfig);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    protected static NativeMemoryConfig newNativeMemoryConfig() {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig();
        memoryConfig.setEnabled(true);
        memoryConfig.setSize(new MemorySize(32, MemoryUnit.MEGABYTES));
        memoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        return memoryConfig;
    }

    static void nativize(NearCacheConfig nearCacheConfig) {
        nearCacheConfig.setInMemoryFormat(NATIVE);
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
        evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy()));
        evictionConfig.setSize(1);
    }

    /**
     * HD backed near cache does not support NONE eviction policy.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheInvalidation_WithNone_whenMaxSizeExceeded() throws Exception {
        super.testNearCacheInvalidation_WithNone_whenMaxSizeExceeded();
    }

    /**
     * HD backed near cache does not support RANDOM eviction policy.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() throws Exception {
        super.testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded();
    }

}
