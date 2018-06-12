package com.hazelcast.map.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.Address;
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

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapHotRestartClearDestroyTest extends AbstractMapHotRestartTest {

    private static final int OPERATION_COUNT = 10000;

    @Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false},
        });
    }

    @Test
    public void test_clear() {
        test(new MapAction() {
            @Override
            public int run(IMap map) {
                map.clear();
                return 0;
            }
        });
    }

    @Test
    public void test_destroy() {
        test(new MapAction() {
            @Override
            public int run(IMap map) {
                map.destroy();
                return 0;
            }
        });
    }

    private void test(MapAction action) {
        Address address = factory.nextAddress();
        Config config = makeConfig(address, 1);
        HazelcastInstance hz = newHazelcastInstance(address, config);
        IMap<Integer, String> map = createMap(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            map.put(key, randomString());
        }

        int expectedSize = action.run(map);

        hz = restartHazelcastInstance(hz, config);
        map = createMap(hz);

        assertEqualsStringFormat("Expected %s map entries, but found %d", expectedSize, map.size());
    }

    private interface MapAction {
        int run(IMap map);
    }
}
