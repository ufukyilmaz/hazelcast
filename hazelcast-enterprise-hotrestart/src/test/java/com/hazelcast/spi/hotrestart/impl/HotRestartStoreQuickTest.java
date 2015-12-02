package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.impl.testsupport.TestProfile;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hotRestartHome;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartStoreQuickTest {

    @Rule public final TestName testName = new TestName();

    @Test public void onHeapTest() throws Exception {
        quickTest(false);
    }

    @Test public void offHeapTest() throws Exception {
        quickTest(true);
    }

    private void quickTest(boolean offHeap) throws Exception {
        final File testingHome = hotRestartHome(getClass(), testName);
        final TestProfile p = new TestProfile.Default();
        p.testCycleCount = 2;
        p.exerciseTimeSeconds = 10;
        p.clearIntervalSeconds = 4;
        p.prefixCount = 10;
        p.keysetSize = 30 * 1000;
        p.hotSetFraction = 1;
        p.logItersHotSetChange = 31;
        p.logMinSize = 7;
        p.sizeIncreaseSteps = 15;
        p.logStepSize = 1;
        p.offHeapMb = offHeap ? 1024 : 0;
        p.offHeapMetadataPercentage = 12f;
        p.restartCount = 0;
        p.disableIo = false;
        try {
            new HotRestartStoreExerciser(testingHome, p).proceed();
        } finally {
            delete(testingHome);
        }
    }
}
