package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.QueryCache;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientQueryCacheIndexTest extends HazelcastTestSupport {

    @Before
    public void setUp() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT, "1");

        Hazelcast.newHazelcastInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_keySet_withPredicate() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        // populate map before construction of query cache.
        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        // populate map after construction of query cache.
        for (int i = putCount; i < 2 * putCount; i++) {
            map.put(i, new Employee(i));
        }

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * putCount - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);
    }


    @Test
    public void test_keySet_withPredicate_whenValuesAreNotCached() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig.setDelaySeconds(1);
        queryCacheConfig.setBatchSize(3);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        // populate map before construction of query cache.
        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        cache.addIndex("__key", true);

        // populate map after construction of query cache.
        for (int i = putCount; i < 2 * putCount; i++) {
            map.put(i, new Employee(i));
        }

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * putCount - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("__key >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_keySet_withPredicate_afterRemovals() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #keySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);

    }


    @Test
    public void test_entrySet_withPredicate_whenValuesNotCached() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        cache.addIndex("id", true);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);
    }

    @Test
    public void test_entrySet_onIndexedKeys_whenValuesNotCached() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        // here add index to key. (key --> integer; value --> Employee)
        cache.addIndex("__key", true);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }

    @Test
    public void test_values_withoutIndex_whenValuesNotCached() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 0;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }

    @Test
    public void test_values_withoutIndex_whenValuesCached() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) client.getMap(mapName);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }

    private void assertKeySetSizeEventually(final int expectedSize, final Predicate predicate, final QueryCache<Integer, Employee> cache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = cache.size();
                Set<Integer> keySet = cache.keySet(predicate);
                assertEquals("cache size = " + size, expectedSize, keySet.size());
            }
        };

        assertTrueEventually(task, 15);
    }

    private void assertEntrySetSizeEventually(final int expectedSize, final Predicate predicate, final QueryCache<Integer, Employee> cache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = cache.size();
                Set<Map.Entry<Integer, Employee>> entries = cache.entrySet(predicate);
                assertEquals("cache size = " + size, expectedSize, entries.size());
            }
        };

        assertTrueEventually(task, 15);
    }

    private void assertValuesSizeEventually(final int expectedSize, final Predicate predicate, final QueryCache<Integer, Employee> cache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = cache.size();
                Collection<Employee> values = cache.values(predicate);
                assertEquals("cache size = " + size, expectedSize, values.size());
            }
        };

        assertTrueEventually(task, 15);
    }
}
