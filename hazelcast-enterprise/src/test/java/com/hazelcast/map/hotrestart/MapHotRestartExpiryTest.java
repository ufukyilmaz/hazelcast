package com.hazelcast.map.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.Address;
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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
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
        Address address = factory.nextAddress();
        Config hzConfig = makeConfig(address, 1);
        HazelcastInstance hz = newHazelcastInstance(address, hzConfig);
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

        hz = restartHazelcastInstance(hz, hzConfig);
        map = createMap(hz);

        assertEquals(0, map.size());
    }
}
