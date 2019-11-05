package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheOutOfMemoryTest extends CommonNearCacheTestSupport {

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;
    private NearCacheManager nearCacheManager;
    private EnterpriseSerializationService ess;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
        ess = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
        nearCacheManager = new HiDensityNearCacheManager(ess, createTaskScheduler(),
                null, new HazelcastProperties(new Properties()));
    }

    @After
    public void tearDown() {
        if (nearCacheManager != null) {
            nearCacheManager.destroyAllNearCaches();
        }
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createNearCacheConfig(name, inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    private NearCacheConfig createNearCacheConfig(String name) {
        return createNearCacheConfig(name, InMemoryFormat.NATIVE);
    }

    private NearCache<Integer, Object> createNearCache(String name) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(name);
        return nearCacheManager.getOrCreateNearCache(name, nearCacheConfig);
    }

    @Test
    public void putToNearCacheShouldNotGetOOMEIfNativeMemoryIsFullAndThereIsNoRecordToEvict() {
        NearCache<Integer, Object> nearCache1 = createNearCache("Near-Cache-1");
        NearCache<Integer, Object> nearCache2 = createNearCache("Near-Cache-2");

        byte[] smallValue = new byte[8 * 1024];
        byte[] bigValue = new byte[NativeMemoryConfig.DEFAULT_PAGE_SIZE / 2]; // 2 MB = Smaller than page size (4 MB)

        int smallValuePutCount = (int) ((memoryManager.getMemoryStats().getMaxNative() / smallValue.length) * 2);
        int bigValuePutCount = (int) ((memoryManager.getMemoryStats().getMaxNative() / bigValue.length) * 2);

        // fill up memory with Near Cache 1
        for (int i = 0; i < smallValuePutCount; i++) {
            nearCache1.put(i, ess.toData(i), smallValue, ess.toData(smallValue));
        }

        // then put a big value to Near Cache 2 and there should not be OOME
        // since eviction is done on other Near Caches until there is enough space for new put
        for (int i = 0; i < bigValuePutCount; i++) {
            nearCache2.put(i, ess.toData(i), bigValue, ess.toData(bigValue));
        }
    }
}
