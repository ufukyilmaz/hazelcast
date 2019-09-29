package com.hazelcast.cache.impl.hotrestart;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheHotRestartExpiryTest extends AbstractCacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false},
        });
    }

    @Test
    public void test() {
        int expireAfter = 10;
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(expireAfter, expireAfter, expireAfter, MILLISECONDS);
        HazelcastInstance hz = newHazelcastInstance();
        Config config = hz.getConfig();
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

        hz = restartInstances(1)[0];
        cache = createCache(hz);

        assertEquals(0, cache.size());
    }
}
