package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.hotrestart.impl.testsupport.MockStoreRegistry;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createStoreRegistry;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hotRestartHome;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hrStoreConfig;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.logger;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.metricsRegistry;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GcExecutorSchedulingTest {
    @Rule public final TestName testName = new TestName();

    @Test public void test() throws Exception {
        final File testingHome = hotRestartHome(getClass(), testName);
        final TestProfile p = new TestProfile.Default();
        p.prefixCount = 10;
        p.keysetSize = 1000;
        p.offHeapMb = 0;
        p.disableIo = true;
        MockStoreRegistry reg = null;
        try {
            reg = createStoreRegistry(hrStoreConfig(testingHome).setIoDisabled(true), null);
            final byte[] value = new byte[1];
            for (int i = 0; i < 20; i++) {
                reg.put(1, 1, value);
                LockSupport.parkNanos(i * 1000);
            }
            LockSupport.parkNanos(MILLISECONDS.toNanos(1));
        } finally {
            if (reg != null) {
                reg.closeHotRestartStore();
                reg.disposeRecordStores();
            }
            delete(testingHome);
        }
    }

}
