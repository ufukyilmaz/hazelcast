package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_HD_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.TWO_GB_V2_HD_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.V4_LICENSE_WITH_HD_MEMORY_DISABLED;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheNativeMemoryLicenseTest extends HazelcastTestSupport {

    private HazelcastServerCachingProvider provider;
    private Config config;

    @Before
    public void setup() {
        config = new Config();
        config.getNativeMemoryConfig().setEnabled(true);
        config.getNativeMemoryConfig().setSize(MemorySize.parse("1", MemoryUnit.GIGABYTES));
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(ENTERPRISE_HD_LICENSE);
    }

    @After
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_hd_memory_is_compatible_with_v2_enterprise_license() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(TWO_GB_V2_HD_LICENSE);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        configureCacheWithNativeMemory(factory);
        factory.newHazelcastInstance(config);
        // this node does not join
        factory.newHazelcastInstance(config);
    }

    @Test(expected = IllegalStateException.class)
    public void test_hd_memory_is_compatible_with_v2_security_only_license() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(SECURITY_ONLY_LICENSE);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        // this node should not start with HD memory
        configureCacheWithNativeMemory(factory);
    }

    @Test
    public void node_should_join_when_max_memory_does_not_exceed_licensed_value() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        configureCacheWithNativeMemory(factory);
        factory.newHazelcastInstance(config);
    }

    @Test(expected = IllegalStateException.class)
    public void test_only_hd_memory_disabled_license() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V4_LICENSE_WITH_HD_MEMORY_DISABLED);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        // this node should not start with HD memory
        configureCacheWithNativeMemory(factory);
    }

    @Test
    public void node_should_join_when_max_memory_does_not_exceed_licensed_value_with_V2_license() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(TWO_GB_V2_HD_LICENSE);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        configureCacheWithNativeMemory(factory);
        factory.newHazelcastInstance(config);
    }

    private void configureCacheWithNativeMemory(TestHazelcastInstanceFactory factory) {
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        provider = createServerCachingProvider(instance);
        CacheManager cacheManager = provider.getCacheManager();

        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(90)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        CacheConfig cacheConfig = new CacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);

        String cacheName = randomString();
        cacheManager.createCache(cacheName, cacheConfig);
    }
}
