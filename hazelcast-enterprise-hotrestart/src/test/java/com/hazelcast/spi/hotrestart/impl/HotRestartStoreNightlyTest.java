package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.impl.testsupport.TestProfile;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hotRestartHome;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class, ParallelTest.class})
public class HotRestartStoreNightlyTest {

    @Rule public final TestName testName = new TestName();

    @Test public void onHeapTest() throws Exception {
        longTest(false);
    }

    @Test public void offHeapTest() throws Exception {
        longTest(true);
    }

    private void longTest(boolean offHeap) throws Exception {
        final File testingHome = hotRestartHome(getClass(), testName);
        final TestProfile p = new TestProfile();
        p.testCycleCount = 20;
        p.exerciseTimeSeconds = 30;
        p.prefixCount = 10;
        p.keysetSize = (offHeap ? 40 : 30) * 1000;
        p.hotSetFraction = 1;
        p.logItersHotSetChange = 31;
        p.logMinSize = 7;
        p.sizeIncreaseSteps = 15;
        p.logStepSize = 1;
        p.clearIntervalSeconds = 7;
        p.offHeapMb = offHeap ? 1024 : 0;
        p.offHeapMetadataPercentage = 15f;
        p.restartCount = 0;
        p.disableIo = false;
        try {
            new HotRestartStoreExerciser(testingHome, p).proceed();
        } finally {
            delete(testingHome);
        }
    }

}
