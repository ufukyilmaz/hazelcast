package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheOutOfMemoryTest extends CommonNearCacheTestSupport {

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;
    private NearCacheManager nearCacheManager;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
        nearCacheManager = new HiDensityNearCacheManager();
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (nearCacheManager != null) {
            nearCacheManager.destroyAllNearCaches();
        }
        if (memoryManager != null) {
            memoryManager.destroy();
        }
    }

    @Override
    protected NearCacheContext createNearCacheContext() {
        return new NearCacheContext(
                nearCacheManager,
                new EnterpriseSerializationServiceBuilder()
                        .setMemoryManager(memoryManager)
                        .build(),
                createNearCacheExecutor());
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(name, inMemoryFormat);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);
        nearCacheConfig.setEvictionConfig(evictionConfig);
        return nearCacheConfig;
    }

    private NearCacheConfig createNearCacheConfig(String name) {
        return createNearCacheConfig(name, InMemoryFormat.NATIVE);
    }

    private NearCache<Integer, String> createNearCache(String name) {
        return nearCacheManager.getOrCreateNearCache(name,
                                                     createNearCacheConfig(name),
                                                     createNearCacheContext());
    }

    @Test
    public void putToNearCacheShouldNotGetOOMEIfNativeMemoryIsFullAndThereIsNoRecordToEvict() {
        NearCache nearCache1 = createNearCache("Near-Cache-1");
        NearCache nearCache2 = createNearCache("Near-Cache-2");

        byte[] smallValue = new byte[8 * 1024];
        byte[] bigValue = new byte[NativeMemoryConfig.DEFAULT_PAGE_SIZE / 2]; // 2 MB = Smaller than page size (4 MB)

        int smallValuePutCount =
                (int) ((memoryManager.getMemoryStats().getMaxNativeMemory() / smallValue.length) * 2);
        int bigValuePutCount =
                (int) ((memoryManager.getMemoryStats().getMaxNativeMemory() / bigValue.length) * 2);

        // Fill up memory with Near-Cache-1
        for (int i = 0; i < smallValuePutCount; i++) {
            nearCache1.put(i, smallValue);
        }

        // Then put a big value to Near-Cache-2 and there should not be OOME
        // since eviction is done on other near-caches until there is enough space for new put
        for (int i = 0; i < bigValuePutCount; i++) {
            nearCache2.put(i, bigValue);
        }
    }

}
