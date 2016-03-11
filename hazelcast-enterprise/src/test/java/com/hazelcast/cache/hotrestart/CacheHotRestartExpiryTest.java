package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartExpiryTest extends AbstractCacheHotRestartTest {

    private static final int OPERATION_COUNT = 1000;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false}
        });
    }

    @Test
    public void test() throws Exception {
        final int expireAfter = 10;
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(expireAfter, expireAfter, expireAfter, TimeUnit.MILLISECONDS);
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, String> cache = createCache(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            cache.put(key, randomString(), expiryPolicy);
        }

        sleepAtLeastMillis(expireAfter);
        final ICache<Integer, String> finalCache = cache;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int key = 0; key < OPERATION_COUNT; key++) {
                    assertNull(finalCache.get(key));
                }
            }
        });
        assertEquals(0, cache.size());

        hz = restartHazelcastInstance(hz);

        cache = createCache(hz);

        assertEquals(0, cache.size());
    }
}
