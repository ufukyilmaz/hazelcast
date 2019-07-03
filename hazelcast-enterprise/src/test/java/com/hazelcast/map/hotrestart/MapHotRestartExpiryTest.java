package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapHotRestartExpiryTest extends AbstractMapHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false},
        });
    }

    @Test
    public void test() {
        int ttl = 10;
        HazelcastInstance hz = newHazelcastInstance();
        IMap<Integer, String> map = createMap(hz);

        for (int key = 0; key < KEY_COUNT; key++) {
            map.put(key, randomString(), ttl, TimeUnit.MILLISECONDS);
        }

        sleepAtLeastMillis(ttl);
        final IMap<Integer, String> finalMap = map;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int key = 0; key < KEY_COUNT; key++) {
                    assertNull(finalMap.get(key));
                }
            }
        });
        assertEquals(0, map.size());

        hz = restartInstances(1)[0];
        map = createMap(hz);

        assertEquals(0, map.size());
    }
}
