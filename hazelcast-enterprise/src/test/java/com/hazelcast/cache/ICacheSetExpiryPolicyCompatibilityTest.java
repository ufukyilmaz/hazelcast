package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ICacheSetExpiryPolicyCompatibilityTest extends HazelcastTestSupport {

    CompatibilityTestHazelcastInstanceFactory factory;
    String cacheName;

    @Before
    public void setup() {
        cacheName = randomName();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testExpiryPolicyIsKeptAfterClusterUpgrade() {
        final int ENTRY_COUNT = 1000;
        HazelcastInstance[] instances = new HazelcastInstance[3];
        factory = new CompatibilityTestHazelcastInstanceFactory();
        instances[0] = factory.newHazelcastInstance();
        instances[1] = factory.newHazelcastInstance();
        instances[2] = factory.newHazelcastInstance();

        instances[0].shutdown();
        instances[1].shutdown();

        waitClusterForSafeState(instances[2]);

        ICache<String, String> cache = createCache(instances[2]);
        //we have a 3.11 member with 3.10 cluster version, its cache records should be lacking expiryPolicy field
        putCache(cache, 0, ENTRY_COUNT);

        getClusterService(instances[2]).changeClusterVersion(Version.of(CURRENT_VERSION));

        setExpiryPolicy(cache, new AccessedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 1)), 0, ENTRY_COUNT);

        //we start a new node to verify that cache records do not lose expiry policy information during migration
        instances[0] = factory.newHazelcastInstance();

        //trigger new expiry policy
        accessEntries(cache, 0, ENTRY_COUNT);
        assertAllNullEventually(cache, 0, ENTRY_COUNT);
    }

    // Create a CachingProvider off currentVersionInstance, then create a new Cache with a typed CacheConfig
    private ICache<String, String> createCache(HazelcastInstance instance) {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(null, null,
                HazelcastCachingProvider.propertiesByInstanceItself(instance));
        return cacheManager.createCache(cacheName, createCacheConfig()).unwrap(ICache.class);
    }

    private void putCache(ICache cache, int from, int to) {
        for (int i = from; i < to; i++) {
            cache.put(Integer.toString(i), Integer.toString(i));
        }
    }

    private void setExpiryPolicy(ICache cache, ExpiryPolicy expiryPolicy, int from, int to) {
        for (int i = from; i < to; i++) {
            cache.setExpiryPolicy(Integer.toString(i), expiryPolicy);
        }
    }

    private void accessEntries(ICache cache, int from, int to) {
        for (int i = from; i < to; i++) {
            cache.get(Integer.toString(i));
        }
    }

    private void assertAllNullEventually(final ICache cache, final int from, final int to) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = from; i < to; i++) {
                    assertNull(cache.get(Integer.toString(i)));
                }
            }
        });
    }

    private CacheConfig<String, String> createCacheConfig() {
        CacheConfig<String, String> cacheConfig = new CacheConfig();
        cacheConfig.setTypes(String.class, String.class);
        return cacheConfig;
    }
}
