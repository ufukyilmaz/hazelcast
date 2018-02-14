package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartCloseDestroyTest extends AbstractCacheHotRestartTest {

    private static final int OPERATION_COUNT = 10000;

    @Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false}
        });
    }

    @Test
    public void test_clear() {
        test(new CacheAction() {
            @Override
            public int run(ICache cache) {
                cache.clear();
                return 0;
            }
        });
    }

    @Test
    public void test_destroy() {
        test(new CacheAction() {
            @Override
            public int run(ICache cache) {
                cache.destroy();
                return 0;
            }
        });
    }

    @Test
    public void test_close() {
        test(new CacheAction() {
            @Override
            public int run(ICache cache) {
                final int size = cache.size();
                cache.close();
                return size;
            }
        });
    }

    private void test(CacheAction action) {
        Config hzConfig = makeConfig(factory.nextAddress());
        HazelcastInstance hz = newHazelcastInstance(hzConfig);
        ICache<Integer, String> cache = createCache(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            cache.put(key, randomString());
        }

        int expectedSize = action.run(cache);

        hz = restartHazelcastInstance(hz, hzConfig);

        cache = createCache(hz);

        assertEquals(expectedSize, cache.size());
    }

    private interface CacheAction {
        int run(ICache cache);
    }
}
