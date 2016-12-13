package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.testsupport.MockStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.testsupport.TestProfile;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hrStoreConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartStoreQuickTest {

    @Rule public final TestName testName = new TestName();

    private File testingHome;
    private TestProfile profile;

    @Before public void setup() {
        testingHome = isolatedFolder(getClass(), testName);
        delete(testingHome);
        profile = new TestProfile.Default();
    }

    @Test public void onHeapExercise() throws Exception {
        setupGeneralProfile();
        profile.offHeapMb = 0;
        generalExercise();
    }

    @Test public void offHeapExercise() throws Exception {
        setupGeneralProfile();
        profile.offHeapMb = 1024;
        generalExercise();
    }

    @Test public void testRemoveAllKeys() throws Exception {
        profile.keysetSize = 100;
        final HotRestartStoreConfig cfg = hrStoreConfig(testingHome);
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
    public void checkMaxChunkSeqOnRestart() throws Exception {
        final HotRestartStoreConfig cfg = hrStoreConfig(testingHome);
        MockStoreRegistry reg = new MockStoreRegistry(cfg, null, true);
        GcHelper helper = ((ConcurrentHotRestartStore) reg.hrStore).getDi().get(GcHelper.class);
        reg.put(1, 1, new byte[1]);
        reg.put(1, 2, new byte[1]);
        reg.put(1, 3, new byte[1]);
        reg.clear(1);
        assertEquals(3, helper.recordSeq());
        closeAndDispose(reg);

        IOUtil.delete(new File(testingHome, "value"));
        reg = new MockStoreRegistry(cfg, null, true);
        helper = ((ConcurrentHotRestartStore) reg.hrStore).getDi().get(GcHelper.class);
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

    private void generalExercise() throws Exception {
        try {
            new HotRestartStoreExerciser(testingHome, profile).proceed();
        } finally {
            delete(testingHome);
        }
    }
}
