package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
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

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.enterprise.SampleLicense.SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.TWO_GB_NATIVE_MEMORY_LICENSE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheNativeMemoryLicenseTest extends HazelcastTestSupport {

    HazelcastServerCachingProvider provider;
    Cache cache;
    Config config;

    @Before
    public void setup() {
        config = new Config();
        config.getNativeMemoryConfig().setEnabled(true);
        config.getNativeMemoryConfig().setSize(MemorySize.parse("1", MemoryUnit.GIGABYTES));
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(TWO_GB_NATIVE_MEMORY_LICENSE);
    }

    @After
    public void tearDown() {
        provider.close();
    }

    @Test(expected = IllegalStateException.class)
    public void node_should_not_join_when_max_memory_exceeds_licensed_value() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        configureCacheWithNativeMemory(factory);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);//This node does not join.
    }

    @Test(expected = IllegalStateException.class)
    public void node_should_not_start_with_security_only_license() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(SECURITY_ONLY_LICENSE);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        configureCacheWithNativeMemory(factory);
        factory.newHazelcastInstance(config);//This node should not start with HD memory
    }

    @Test
    public void node_should_join_when_max_memory_does_not_exceed_licensed_value() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        configureCacheWithNativeMemory(factory);
        factory.newHazelcastInstance(config);
    }

    private void configureCacheWithNativeMemory(TestHazelcastInstanceFactory factory) {
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        provider = HazelcastServerCachingProvider.createCachingProvider(instance);
        CacheManager cacheManager = provider.getCacheManager();
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setSize(90);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        cacheConfig.setEvictionConfig(evictionConfig);
        String cacheName = randomString();
        cache = cacheManager.createCache(cacheName,cacheConfig);
    }

}
