package com.hazelcast.cache;

import classloading.domain.Person;
import classloading.domain.PersonEntryProcessor;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.processor.EntryProcessor;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getCurrentVersion;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.getOldestKnownVersion;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class CacheWithTypedConfigCompatibilityTest extends HazelcastTestSupport {

    CompatibilityTestHazelcastInstanceFactory factory;
    HazelcastInstance[] instances;
    HazelcastInstance currentVersionInstance;
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
    public void testTypedCacheConfig_worksOnPreviousClusterVersion() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
        instances = factory.newInstances();
        currentVersionInstance = instances[instances.length - 1];
        Cache<String, Person> cache = createCache();

        assertCacheWorks(cache);
    }

    @Test
    public void testPreviousVersionMember_joinsCurrentVersionMaster_withTypedCacheConfig() {
        String[] versions = new String[]{getOldestKnownVersion(), getCurrentVersion(), getOldestKnownVersion()};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);
        // start previous & current instances
        instances = factory.newInstances(null, versions.length - 1);
        currentVersionInstance = instances[instances.length - 1];
        Cache<String, Person> cache = createCache();

        assertCacheWorks(cache);

        // shutdown oldest member
        instances[0].shutdown();
        // start once more previous member, this time current member is cluster master
        instances[0] = factory.newHazelcastInstance();
        assertClusterSizeEventually(2, currentVersionInstance);
    }

    @Test
    public void testCurrentVersionMember_joinsCurrentVersionMasterWithTypedCacheConfig_afterUpgrade() {
        String[] versions = new String[]{getOldestKnownVersion(), getCurrentVersion(), getCurrentVersion()};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);
        // start previous & current instances
        instances = factory.newInstances(null, versions.length - 1);
        currentVersionInstance = instances[instances.length - 1];
        Cache<String, Person> cache = createCache();

        assertCacheWorks(cache);

        // shutdown oldest member
        instances[0].shutdown();
        waitClusterForSafeState(currentVersionInstance);
        currentVersionInstance.getCluster().changeClusterVersion(Versions.CURRENT_CLUSTER_VERSION);

        // start a new current member, this time current member is cluster master and has been upgraded
        instances[0] = factory.newHazelcastInstance();
        assertClusterSizeEventually(2, currentVersionInstance);
    }

    // Create a CachingProvider off currentVersionInstance, then create a new Cache with a typed CacheConfig
    private Cache<String, Person> createCache() {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(currentVersionInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        return cacheManager.createCache(cacheName, new CacheConfig().setTypes(String.class, Person.class));
    }

    private void assertCacheWorks(Cache cache) {
        EntryProcessor<String, Person, Person> entryProcessor = new PersonEntryProcessor();
        for (int i = 0; i < 1000; i++) {
            cache.put(Integer.toString(i), new Person());
            cache.invoke(Integer.toString(i), entryProcessor);
        }
    }
}
