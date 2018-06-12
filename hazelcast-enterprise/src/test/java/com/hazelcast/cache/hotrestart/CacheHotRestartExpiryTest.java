package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
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
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartExpiryTest extends AbstractCacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, KEY_COUNT, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false},
        });
    }

    @Test
    public void test() {
        int expireAfter = 10;
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(expireAfter, expireAfter, expireAfter, MILLISECONDS);
        Config hzConfig = makeConfig(factory.nextAddress());
        HazelcastInstance hz = newHazelcastInstance(hzConfig);
        ICache<Integer, String> cache = createCache(hz);

        for (int key = 0; key < KEY_COUNT; key++) {
            cache.put(key, randomString(), expiryPolicy);
        }

        sleepAtLeastMillis(expireAfter);
        final ICache<Integer, String> finalCache = cache;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int key = 0; key < KEY_COUNT; key++) {
                    assertNull(finalCache.get(key));
                }
            }
        });
        assertEquals(0, cache.size());

        hz = restartHazelcastInstance(hz, hzConfig);
        cache = createCache(hz);

        assertEquals(0, cache.size());
    }
}
