package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientQueryCacheMethodsWithPredicateTest extends HazelcastTestSupport {

    private static final int DEFAULT_TEST_TIMEOUT = 120;

    @BeforeClass
    public static void setUp() throws Exception {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }


    @Test
    public void test_keySet() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        // populate map before construction of query cache.
        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        // populate map after construction of query cache.
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_keySet_whenIncludeValueFalse() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        // populate map before construction of query cache.
        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        cache.addIndex("__key", true);

        // populate map after construction of query cache.
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("__key >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_keySet_onRemovedIndexes() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #keySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);

    }


    @Test
    public void test_entrySet() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        // populate map before construction of query cache.
        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        // populate map after construction of query cache.
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_entrySet_whenIncludeValueFalse() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        cache.addIndex("id", true);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);
    }

    @Test
    public void test_entrySet_withIndexedKeys_whenIncludeValueFalse() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        // here adding key index. (key --> integer; value --> Employee)
        cache.addIndex("__key", true);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }


    @Test
    public void test_values() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        // populate map before construction of query cache.
        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        // populate map after construction of query cache.
        for (int i = count; i < 2 * count; i++) {
            map.put(i, new Employee(i));
        }

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);

    }

    @Test
    public void test_values_withoutIndex() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }

    @Test
    public void test_values_withoutIndex_whenIncludeValueFalse() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = getInstance();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);

        for (int i = 17; i < count; i++) {
            map.remove(i);
        }

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 0;
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

        assertTrueEventually(task, DEFAULT_TEST_TIMEOUT);
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

        assertTrueEventually(task, DEFAULT_TEST_TIMEOUT);
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

        assertTrueEventually(task, DEFAULT_TEST_TIMEOUT);
    }

    private HazelcastInstance getInstance() {
        return HazelcastClient.newHazelcastClient();
    }
}
