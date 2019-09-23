package com.hazelcast.internal.memory;

import com.hazelcast.config.Config;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.memory.FreeMemoryChecker.FREE_MEMORY_CHECKER_ENABLED;
import static com.hazelcast.internal.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.internal.memory.MemoryStatsSupport.totalPhysicalMemory;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FreeMemoryCheckerTest {

    @Rule
    public ExpectedException rule = ExpectedException.none();

    private FreeMemoryChecker freeMemoryChecker;

    @Before
    public void setUp() {
        Config config = new Config();
        config.setProperty(FREE_MEMORY_CHECKER_ENABLED.getName(), "true");

        HazelcastProperties properties = new HazelcastProperties(config);

        freeMemoryChecker = new FreeMemoryChecker(properties);
    }

    @Test
    public void checkFreeMemory_withZero() {
        freeMemoryChecker.checkFreeMemory(0);
    }

    @Test
    public void checkFreeMemory_withMaxLong() {
        if (totalPhysicalMemory() >= 0 && freePhysicalMemory() >= 0) {
            // if we get valid numbers here, we expect the memory check to fail
            rule.expect(NativeOutOfMemoryError.class);
        }

        freeMemoryChecker.checkFreeMemory(Long.MAX_VALUE);
    }

    @Test
    public void checkFreeMemory_whenDisabled() {
        System.setProperty(FREE_MEMORY_CHECKER_ENABLED.getName(), "false");
        freeMemoryChecker = new FreeMemoryChecker();

        freeMemoryChecker.checkFreeMemory(Long.MAX_VALUE);
    }
}
