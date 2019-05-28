package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheHotRestartCloseDestroyTest extends AbstractCacheHotRestartTest {

    static final int OPERATION_COUNT = 10000;

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false, false},
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
                int size = cache.size();
                cache.close();
                return size;
            }
        });
    }

    private void test(CacheAction action) {
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, String> cache = createCache(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            cache.put(key, randomString());
        }

        int expectedSize = action.run(cache);

        hz = restartInstances(1)[0];
        cache = createCache(hz);

        assertEqualsStringFormat("Expected %s cache entries, but found %d", expectedSize, cache.size());
    }

    private interface CacheAction {
        int run(ICache cache);
    }
}
