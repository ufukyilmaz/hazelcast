package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.UUID;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_CLEAR_INTERVAL_SECONDS;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_DISABLE_IO;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_HOTSET_FRACTION;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_ITERATIONS_K;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_JUST_START;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_LOG_ITERS_HOTSET_CHANGE;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_LOG_MIN_SIZE;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_LOG_SIZE_STEP;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_KEY_COUNT_K;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_OFFHEAP_MB;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_PREFIX_COUNT;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_SIZE_INCREASE_STEPS;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.PROP_TEST_CYCLE_COUNT;
import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreExerciser.toProps;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class, ParallelTest.class})
public class HotRestartStoreNightlyTest {
    @Test
    public void onHeapTest() throws Exception {
        longTest(false);
    }

    @Test
    public void offHeapTest() throws Exception {
        longTest(true);
    }

    private static void longTest(boolean offHeap) throws Exception {
        final File testingHome = new File("nightlytest-" + UUID.randomUUID());
        try {
            new HotRestartStoreExerciser(testingHome,
                    toProps(PROP_TEST_CYCLE_COUNT, "10",
                            PROP_PREFIX_COUNT, "14",
                            PROP_KEY_COUNT_K, "50",
                            PROP_HOTSET_FRACTION, "1",
                            PROP_LOG_ITERS_HOTSET_CHANGE, "31",
                            PROP_LOG_MIN_SIZE, "7",
                            PROP_SIZE_INCREASE_STEPS, "5",
                            PROP_LOG_SIZE_STEP, "3",
                            PROP_ITERATIONS_K, "50000",
                            PROP_CLEAR_INTERVAL_SECONDS, "7",
                            PROP_OFFHEAP_MB, offHeap ? "512" : "0",
                            PROP_JUST_START, "0",
                            PROP_DISABLE_IO, "false"))
                    .proceed();
        } finally {
            delete(testingHome);
        }
    }
}
