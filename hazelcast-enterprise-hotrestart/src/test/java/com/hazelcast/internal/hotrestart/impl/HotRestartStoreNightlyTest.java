package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.internal.hotrestart.impl.testsupport.TestProfile;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;

import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.internal.nio.IOUtil.delete;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class HotRestartStoreNightlyTest {

    @Rule
    public final TestName testName = new TestName();

    private File testingHome;

    @Parameters(name = "encrypted:{0}")
    public static Object[] data() {
        return new Object[] { false, true };
    }

    @Parameter
    public boolean encrypted;

    @Before
    public void setUp() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
    }

    @After
    public void tearDown() {
        delete(testingHome);
    }

    @Test(timeout = 15 * 60 * 1000)
    public void onHeapTest() {
        exercise(false);
    }

    @Test(timeout = 15 * 60 * 1000)
    public void offHeapTest() {
        exercise(true);
    }

    private void exercise(boolean offHeap) {
        final TestProfile p = new TestProfile.Default();
        p.testCycleCount = 20;
        p.exerciseTimeSeconds = 30;
        p.prefixCount = 10;
        p.keysetSize = 30 * 1000;
        p.hotSetFraction = 1;
        p.logItersHotSetChange = 31;
        p.logMinSize = 7;
        p.sizeIncreaseSteps = 15;
        p.logStepSize = 1;
        p.clearIntervalSeconds = 7;
        p.offHeapMb = offHeap ? 1024 : 0;
        p.offHeapMetadataPercentage = 15f;
        p.restartCount = 0;
        p.encrypted = encrypted;
        new HotRestartStoreExerciser(testingHome, p).proceed();
    }
}