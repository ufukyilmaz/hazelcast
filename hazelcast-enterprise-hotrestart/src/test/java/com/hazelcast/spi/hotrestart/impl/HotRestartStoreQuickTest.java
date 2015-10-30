package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartStoreQuickTest {
    @Test
    public void testOnHeap() throws Exception {
        quickTest(false);
    }

    @Test
    public void testOffHeap() throws Exception {
        quickTest(false);
    }

    private static void quickTest(boolean offHeap) throws Exception {
        final File testingHome = new File("quicktest-" + UUID.randomUUID());
        try {
            new HotRestartStoreExerciser(testingHome,
                    toProps(PROP_TEST_CYCLE_COUNT, "2",
                            PROP_PREFIX_COUNT, "14",
                            PROP_KEY_COUNT_K, "20",
                            PROP_HOTSET_FRACTION, "1",
                            PROP_LOG_ITERS_HOTSET_CHANGE, "31",
                            PROP_LOG_MIN_SIZE, "7",
                            PROP_SIZE_INCREASE_STEPS, "5",
                            PROP_LOG_SIZE_STEP, "3",
                            PROP_ITERATIONS_K, "1000",
                            PROP_CLEAR_INTERVAL_SECONDS, "2",
                            PROP_OFFHEAP_MB, offHeap ? "64" : "0",
                            PROP_JUST_START, "0",
                            PROP_DISABLE_IO, "false"))
                    .proceed();
        } finally {
            delete(testingHome);
        }
    }
}
