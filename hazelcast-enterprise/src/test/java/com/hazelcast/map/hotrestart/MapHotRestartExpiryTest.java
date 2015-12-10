package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapHotRestartExpiryTest extends AbstractMapHotRestartTest {

    private static final int OPERATION_COUNT = 1000;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false}
        });
    }

    @Test
    public void test() throws Exception {
        int ttl = 10;
        HazelcastInstance hz = newHazelcastInstance();
        IMap<Integer, String> map = createMap(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            map.put(key, randomString(), ttl, TimeUnit.MILLISECONDS);
        }

        sleepAtLeastMillis(ttl);
        final IMap<Integer, String> finalMap = map;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int key = 0; key < OPERATION_COUNT; key++) {
                    assertNull(finalMap.get(key));
                }
            }
        });
        assertEquals(0, map.size());

        hz = restartHazelcastInstance(hz);

        map = createMap(hz);

        assertEquals(0, map.size());
    }
}
