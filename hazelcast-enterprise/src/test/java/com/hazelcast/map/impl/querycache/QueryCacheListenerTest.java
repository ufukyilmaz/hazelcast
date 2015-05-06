package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class QueryCacheListenerTest extends AbstractQueryCacheTestSupport {

    @Test
    public void listen_withPredicate_afterQueryCacheCreation() throws Exception {
        String cacheName = randomString();

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(10);
        final QueryCache<Integer, Employee> cache
                = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents),
                new SqlPredicate("id > 100"), true);

        final int count = 111;
        populateMap(map, count);

        assertOpenEventually(numberOfCaughtEvents, 10);

    }


    @Test
    public void listenKey_withPredicate_afterQueryCacheCreation() throws Exception {
        String cacheName = randomString();

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(1);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        final int keyToListen = 109;
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), new SqlPredicate("id > 100"), keyToListen, true);

        final int count = 111;
        populateMap(map, count);

        assertOpenEventually(numberOfCaughtEvents, 10);

    }


    @Test
    public void listenKey_withMultipleListeners_afterQueryCacheCreation() throws Exception {
        final int keyToListen = 109;
        String cacheName = randomString();

        CountDownLatch additionCount = new CountDownLatch(2);
        CountDownLatch removalCount = new CountDownLatch(2);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(additionCount),
                new SqlPredicate("id > 100"), keyToListen, true);
        cache.addEntryListener(new QueryCacheRemovalListener(removalCount),
                new SqlPredicate("id > 100"), keyToListen, true);

        final int count = 111;
        populateMap(map, count);
        removeEntriesFromMap(map, 0, count);
        populateMap(map, count);
        removeEntriesFromMap(map, 0, count);

        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, cache.size());
            }
        };

        assertTrueEventually(task);
        assertOpenEventually(cache.size() + "", additionCount, 10);
        assertOpenEventually(cache.size() + "", removalCount, 10);

    }

    @Test
    public void listenerShouldReceiveValues_whenValueCaching_enabled() throws Exception {
        boolean includeValue = true;
        testValueCaching(includeValue);
    }

    @Test
    public void listenerShouldNotReceiveValues_whenValueCaching_disabled() throws Exception {
        boolean includeValue = false;
        testValueCaching(includeValue);
    }

    @Test
    public void listenerShouldReceive_CLEAR_ALL_Event_whenIMapCleared() throws Exception {
        String cacheName = randomString();
        final int entryCount = 1000;

        final AtomicInteger clearAllEventCount = new AtomicInteger();
        final QueryCache<Integer, Employee> queryCache
                = map.getQueryCache(cacheName, new EntryAdapter() {
            @Override
            public void mapCleared(MapEvent e) {
                clearAllEventCount.incrementAndGet();
            }
        }, TruePredicate.INSTANCE, false);

        populateMap(map, entryCount);

        assertQueryCacheSizeEventually(entryCount, queryCache);

        map.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                //expecting at least 1 event.
                assertTrue(clearAllEventCount.get() >= 1);
                assertEquals(0, queryCache.size());
            }
        });
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache cache) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, cache.size());
            }
        }, 10);
    }

    private void testValueCaching(final boolean includeValue) {
        String cacheName = randomString();

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        final TestIncludeValueListener listener = new TestIncludeValueListener();
        cache.addEntryListener(listener, includeValue);

        final int putCount = 1000;
        populateMap(map, putCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(putCount, cache.size());
                if (includeValue) {
                    assertTrue("There should not be any null value", listener.hasValue);
                } else {
                    assertFalse("There should not be any non-null value", listener.hasValue);
                }
            }
        });
    }

    private class TestIncludeValueListener implements EntryAddedListener {

        volatile boolean hasValue = false;

        @Override
        public void entryAdded(EntryEvent event) {
            Object value = event.getValue();
            hasValue = (value != null);
        }
    }

    private class QueryCacheAdditionListener implements EntryAddedListener {

        private final CountDownLatch numberOfCaughtEvents;

        public QueryCacheAdditionListener(CountDownLatch numberOfCaughtEvents) {
            this.numberOfCaughtEvents = numberOfCaughtEvents;
        }

        @Override
        public void entryAdded(EntryEvent event) {
            numberOfCaughtEvents.countDown();
        }
    }


    private class QueryCacheRemovalListener implements EntryRemovedListener {

        private final CountDownLatch numberOfCaughtEvents;

        public QueryCacheRemovalListener(CountDownLatch numberOfCaughtEvents) {
            this.numberOfCaughtEvents = numberOfCaughtEvents;
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            numberOfCaughtEvents.countDown();
        }
    }

}
