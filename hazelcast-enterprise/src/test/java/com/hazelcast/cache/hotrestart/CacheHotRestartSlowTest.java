package com.hazelcast.cache.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelTest.class})
public class CacheHotRestartSlowTest extends CacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2} clusterSize:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, 1},
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, 3},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, 1},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, 3},
        });
    }
}
