package com.hazelcast.cache.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
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
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelTest.class})
public class CacheForceStartSlowTest extends CacheForceStartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, true, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false},
        });
    }
}
