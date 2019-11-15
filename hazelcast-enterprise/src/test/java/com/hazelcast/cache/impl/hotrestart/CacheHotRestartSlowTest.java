package com.hazelcast.cache.impl.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({SlowTest.class, ParallelJVMTest.class})
public class CacheHotRestartSlowTest extends CacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4} clusterSize:{5}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, false, 1},
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, false, 3},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, false, 1},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, false, 3},
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, true, 1},
        });
    }
}