package com.hazelcast.map.impl.querycache;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheMethodsWithPredicateTest extends AbstractQueryCacheTestSupport {

    @Test
    public void test_keySet_onIndexedField() throws Exception {
        final int count = 111;
        populateMap(map, count);

        String cacheName = randomString();
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        populateMap(map, count, 2 * count);

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);

    }

    @Test
    public void test_keySet_onIndexedField_whenIncludeValueFalse() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        cache.addIndex("__key", true);

        populateMap(map, count, 2 * count);

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("__key >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_keySet_onIndexedField_afterRemovalOfSomeIndexes() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        populateMap(map, 17, count);

        // Just choose arbitrary numbers to prove whether #keySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);

    }


    @Test
    public void test_entrySet() throws Exception {
        final int count = 1;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        populateMap(map, count, 2 * count);

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 0;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_entrySet_whenIncludeValueFalse() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        cache.addIndex("id", true);

        removeEntriesFromMap(map, 17, count);

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("id < " + smallerThan), cache);
    }


    @Test
    public void test_entrySet_withIndexedKeys_whenIncludeValueFalse() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);
        // here adding key index. (key --> integer; value --> Employee)
        cache.addIndex("__key", true);

        removeEntriesFromMap(map, 17, count);

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }

    @Test
    public void test_values() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addIndex("id", true);

        populateMap(map, count, 2 * count);

        // Just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working.
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * count - equalsOrBiggerThan;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("id >= " + equalsOrBiggerThan), cache);

    }


    @Test
    public void test_values_withoutIndex() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);

        removeEntriesFromMap(map, 17, count);

        // Just choose arbitrary numbers to prove whether #entrySet with predicate is working.
        int smallerThan = 17;
        int expectedSize = 17;
        assertValuesSizeEventually(expectedSize, new SqlPredicate("__key < " + smallerThan), cache);

    }

    @Test
    public void test_values_withoutIndex_whenIncludeValueFalse() throws Exception {
        final int count = 111;
        String cacheName = randomString();

        populateMap(map, count);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);

        removeEntriesFromMap(map, 17, count);

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

        assertTrueEventually(task, 10);
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

        assertTrueEventually(task);
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

        assertTrueEventually(task, 10);
    }
}
