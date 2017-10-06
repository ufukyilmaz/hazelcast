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
        String[] versions = new String[] {"3.8", "3.8.1", "3.8.2", "3.8.3", "3.8.4",
                                          "3.8.5", "3.8.6", "3.9"};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);
        instances = factory.newInstances();
        currentVersionInstance = instances[instances.length - 1];
        Cache<String, Person> cache = createCache();

        assertCacheWorks(cache);
    }

    @Test
    public void testPreviousVersionMember_joinsCurrentVersionMaster_withTypedCacheConfig() {
        String[] versions = new String[] {"3.8", "3.9", "3.8"};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);
        // start 3.8 & 3.9 instances
        instances = factory.newInstances(null, versions.length - 1);
        currentVersionInstance = instances[instances.length - 1];
        Cache<String, Person> cache = createCache();

        assertCacheWorks(cache);

        // shutdown oldest member
        instances[0].shutdown();
        // start once more 3.8 member, this time 3.9 member is cluster master and should deliver the OnJoinCacheOp as post-join
        instances[0] = factory.newHazelcastInstance();
        assertClusterSizeEventually(2, currentVersionInstance);
    }

    @Test
    public void testCurrentVersionMember_joinsCurrentVersionMasterWithTypedCacheConfig_afterUpgrade() {
        String[] versions = new String[] {"3.8", "3.9", "3.9"};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);
        // start 3.8 & 3.9 instances
        instances = factory.newInstances(null, versions.length - 1);
        currentVersionInstance = instances[instances.length - 1];
        Cache<String, Person> cache = createCache();

        assertCacheWorks(cache);

        // shutdown oldest member
        instances[0].shutdown();
        waitClusterForSafeState(currentVersionInstance);
        currentVersionInstance.getCluster().changeClusterVersion(Versions.CURRENT_CLUSTER_VERSION);

        // start a new 3.9 member, this time 3.9 member is cluster master and should deliver the OnJoinCacheOp as pre-join
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
