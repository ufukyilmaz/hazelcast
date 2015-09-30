package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientQueryCacheListenerTest extends HazelcastTestSupport {

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
    public void shouldReceiveEvent_whenListening_withPredicate() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(10);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), new SqlPredicate("id > 100"), true);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        assertOpenEventually(numberOfCaughtEvents, 10);

    }


    @Test
    public void shouldReceiveEvent_whenListeningKey_withPredicate() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);


        CountDownLatch numberOfCaughtEvents = new CountDownLatch(1);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        final int keyToListen = 109;
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents),
                new SqlPredicate("id > 100"), keyToListen, true);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        assertOpenEventually(numberOfCaughtEvents, 10);

    }

    @Test
    public void shouldReceiveEvent_whenListeningKey_withMultipleListener() throws Exception {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Employee> map = (IEnterpriseMap) instance.getMap(mapName);

        CountDownLatch additionCount = new CountDownLatch(2);
        CountDownLatch removalCount = new CountDownLatch(2);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        final int keyToListen = 109;
        cache.addEntryListener(new QueryCacheAdditionListener(additionCount), new SqlPredicate("id > 100"), keyToListen, true);
        cache.addEntryListener(new QueryCacheRemovalListener(removalCount), new SqlPredicate("id > 100"), keyToListen, true);

        // populate map before construction of query cache.
        final int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        for (int i = 0; i < count; i++) {
            map.remove(i);
        }

        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        for (int i = 0; i < count; i++) {
            map.remove(i);
        }

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
    public void shouldReceiveValue_whenIncludeValue_enabled() throws Exception {
        boolean includeValue = true;
        testIncludeValue(includeValue);
    }

    @Test
    public void shouldNotReceiveValue_whenIncludeValue_disabled() throws Exception {
        boolean includeValue = false;
        testIncludeValue(includeValue);
    }

    private void testIncludeValue(final boolean includeValue) {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) client.getMap(mapName);

        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        final TestIncludeValueListener listener = new TestIncludeValueListener();
        cache.addEntryListener(listener, includeValue);

        final int putCount = 10;
        for (int i = 0; i < putCount; i++) {
            map.put(i, i);
        }

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
