package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.testsupport.MockStoreRegistry;
import com.hazelcast.internal.hotrestart.impl.testsupport.TestProfile;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.hrStoreConfig;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartStoreQuickTest {

    @Rule
    public final TestName testName = new TestName();

    private File testingHome;
    private TestProfile profile;

    @Parameters(name = "encrypted:{0}")
    public static Object[] data() {
        return new Object[] { false, true };
    }

    @Parameter
    public boolean encrypted;

    @Before
    public void setup() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        profile = new TestProfile.Default();
        profile.encrypted = encrypted;
    }

    @After
    public void tearDown() {
        delete(testingHome);
    }

    @Test
    public void onHeapExercise() {
        setupGeneralProfile();
        profile.offHeapMb = 0;
        new HotRestartStoreExerciser(testingHome, profile).proceed();
    }

    @Test
    public void offHeapExercise() {
        setupGeneralProfile();
        profile.offHeapMb = 1024;
        new HotRestartStoreExerciser(testingHome, profile).proceed();
    }

    @Test
    public void testRemoveAllKeys() {
        profile.keysetSize = 100;
        final HotRestartStoreConfig cfg = hrStoreConfig(testingHome, encrypted);
        MockStoreRegistry reg = new MockStoreRegistry(cfg, null, false);
        putAll(reg);
        removeAll(reg);
        putAll(reg);
        closeAndDispose(reg);
        reg = new MockStoreRegistry(cfg, null, false);
        removePutAll(reg);
        closeAndDispose(reg);
    }

    @Test
    public void checkMaxChunkSeqOnRestart() {
        final HotRestartStoreConfig cfg = hrStoreConfig(testingHome, encrypted);
        MockStoreRegistry reg = new MockStoreRegistry(cfg, null, true);
        GcHelper helper = ((ConcurrentHotRestartStore) reg.hrStore).getDi().get(GcHelper.class);
        assertNotNull(helper);

        reg.put(1, 1, new byte[1]);
        reg.put(1, 2, new byte[1]);
        reg.put(1, 3, new byte[1]);
        reg.clear(1);
        assertEquals(3, helper.recordSeq());
        closeAndDispose(reg);

        delete(new File(testingHome, "value"));
        reg = new MockStoreRegistry(cfg, null, true);
        helper = ((ConcurrentHotRestartStore) reg.hrStore).getDi().get(GcHelper.class);
        assertNotNull(helper);
        assertEquals(3, helper.recordSeq());
        closeAndDispose(reg);
    }

    private void putAll(MockStoreRegistry reg) {
        final byte[] value = new byte[102400];
        for (int key = 0; key < profile.keysetSize; key++) {
            reg.put(1, key + 1, value);
        }
    }

    private void removeAll(MockStoreRegistry reg) {
        for (int key = 0; key < profile.keysetSize; key++) {
            reg.remove(1, key + 1);
        }
    }

    private void removePutAll(MockStoreRegistry reg) {
        for (int i = 0; i < 3; i++) {
            removeAll(reg);
            putAll(reg);
        }
    }

    private static void closeAndDispose(MockStoreRegistry reg) {
        reg.closeHotRestartStore();
        reg.disposeRecordStores();
    }

    private void setupGeneralProfile() {
        profile.testCycleCount = 2;
        profile.exerciseTimeSeconds = 10;
        profile.clearIntervalSeconds = 4;
        profile.prefixCount = 10;
        profile.keysetSize = 30 * 1000;
        profile.hotSetFraction = 1;
        profile.logItersHotSetChange = 31;
        profile.logMinSize = 7;
        profile.sizeIncreaseSteps = 15;
        profile.logStepSize = 1;
        profile.offHeapMetadataPercentage = 12f;
    }
}
