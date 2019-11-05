package com.hazelcast.cache;

import com.hazelcast.NativeMemoryTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.EternalExpiryPolicy;
import java.util.HashSet;
import java.util.Set;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityCacheSetExpiryPolicyLeakTest extends HazelcastTestSupport {

    private final int KEY_COUNT = 10000;
    private final String cacheName = randomMapName();
    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
    }

    @After
    public void teardown() {
        factory.shutdownAll();
    }

    @Override
    public Config getConfig() {
        Config cfg = super.getConfig();
        cfg.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(64, MemoryUnit.MEGABYTES))
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(99)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(1);
        cfg.addCacheConfig(cacheConfig);
        return cfg;
    }

    @Test
    public void testPutSetExpiryPolicyRemoveShouldNotLeaveLeftoverNativeMemory() {
        ICache<String, String> cache = instance1.getCacheManager().getCache(cacheName);

        Set<String> keys = new HashSet<String>();
        for (int i = 0; i < KEY_COUNT; i++) {
            keys.add("key" + i);
        }
        for (String key : keys) {
            cache.put(key, "value");
        }

        for (String key : keys) {
            cache.setExpiryPolicy(key, new EternalExpiryPolicy());
        }

        for (String key : keys) {
            cache.remove(key);
        }

        cache.destroy();
        NativeMemoryTestUtil.assertFreeNativeMemory(instance1, instance2);
    }

    @Test
    public void testMultipleSetExpiryPolicyShouldNotLeak() {
        ICache<String, String> cache = instance1.getCacheManager().getCache(cacheName);

        Set<String> keys = new HashSet<String>();
        for (int i = 0; i < KEY_COUNT; i++) {
            keys.add("key" + i);
        }
        for (String key : keys) {
            cache.put(key, "value");
        }

        for (String key : keys) {
            cache.setExpiryPolicy(key, new EternalExpiryPolicy());
            //intentional duplicate call to see if the old expiry policy is successfully removed from hd memory
            cache.setExpiryPolicy(key, new EternalExpiryPolicy());
        }

        cache.destroy();
        NativeMemoryTestUtil.assertFreeNativeMemory(instance1, instance2);
    }

    @Test
    public void testNewPutOnAKeyWithExpiryPolicyShouldNotLeak() {
        ICache<String, String> cache = instance1.getCacheManager().getCache(cacheName);

        Set<String> keys = new HashSet<String>();
        for (int i = 0; i < KEY_COUNT; i++) {
            keys.add("key" + i);
        }
        for (String key : keys) {
            cache.put(key, "value");
        }

        for (String key : keys) {
            cache.setExpiryPolicy(key, new HazelcastExpiryPolicy(1, 1, 1));
        }

        for (String key : keys) {
            cache.put(key, "value");
        }

        cache.destroy();
        NativeMemoryTestUtil.assertFreeNativeMemory(instance1, instance2);
    }
}
