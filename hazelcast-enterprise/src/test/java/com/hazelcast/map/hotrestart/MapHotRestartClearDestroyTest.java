package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
public class MapHotRestartClearDestroyTest extends AbstractMapHotRestartTest {

    static final int OPERATION_COUNT = 10000;

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false, false, false},
                {InMemoryFormat.BINARY, OPERATION_COUNT, false, false, false},
                {InMemoryFormat.NATIVE, OPERATION_COUNT, false, false, true},
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
        HazelcastInstance hz = newHazelcastInstance();
        IMap<Integer, String> map = createMap(hz);

        for (int key = 0; key < OPERATION_COUNT; key++) {
            map.put(key, randomString());
        }

        int expectedSize = action.run(map);

        hz = restartInstances(1)[0];
        map = createMap(hz);

        assertEqualsStringFormat("Expected %s map entries, but found %d", expectedSize, map.size());
    }

    private interface MapAction {
        int run(IMap map);
    }
}
