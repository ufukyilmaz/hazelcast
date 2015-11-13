package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartCloseDestroyTest extends AbstractCacheHotRestartTest {

    private static final int OPERATION_COUNT = 10000;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false}
        });
    }

    @Test
    public void test_clear() throws Exception {
        test(new CacheAction() {
            @Override
            public int run(ICache cache) {
                cache.clear();
                return 0;
            }
        });
    }

    @Test
    public void test_destroy() throws Exception {
        test(new CacheAction() {
            @Override
            public int run(ICache cache) {
                cache.destroy();
                return 0;
            }
        });
    }

    @Test
    public void test_close() throws Exception {
       test(new CacheAction() {
           @Override
           public int run(ICache cache) {
               final int size = cache.size();
               cache.close();
               return size;
           }
       });
    }

    private void test(CacheAction action) throws Exception {
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, String> cache = createCache(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            cache.put(key, randomString());
        }

        int expectedSize = action.run(cache);

        hz = restartHazelcastInstance(hz);

        cache = createCache(hz);

        assertEquals(expectedSize, cache.size());
    }

    private interface CacheAction {
        int run(ICache cache);
    }
}
