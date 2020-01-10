package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.internal.nearcache.impl.AbstractNearCacheBasicTest.DEFAULT_NEAR_CACHE_NAME;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDNearCacheMemoryManagerTest extends HazelcastTestSupport {

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService ess;
    private HazelcastProperties properties;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
        ess = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
        properties = new HazelcastProperties(new Properties());
    }

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
            memoryManager = null;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void putToNearCache_throws_illegal_state_exception_if_memory_manager_was_disposed() {
        memoryManager.dispose();

        createHDNearCache(ess, properties).put(1, ess.toData(1), 1, ess.toData(1));
    }

    private static <K, V> NearCache<K, V> createHDNearCache(EnterpriseSerializationService ess,
                                                            HazelcastProperties properties) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        DelegatingTaskScheduler taskScheduler = new DelegatingTaskScheduler(executorService, executorService);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        return HDNearCacheTest.createHDNearCache(ess, taskScheduler, null, nearCacheConfig, properties);
    }

    protected static NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return new NearCacheConfig()
                .setName(name)
                .setInMemoryFormat(inMemoryFormat).setEvictionConfig(evictionConfig);
    }
}
