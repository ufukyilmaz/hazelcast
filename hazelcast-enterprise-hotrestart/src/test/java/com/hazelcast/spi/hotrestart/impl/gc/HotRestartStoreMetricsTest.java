package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupRunnable;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupTestSupport;
import com.hazelcast.spi.hotrestart.impl.testsupport.MockStoreRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.concurrent.Callable;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.TOMB_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hotRestartHome;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartStoreMetricsTest extends HazelcastTestSupport {

    @Rule public final TestName testName = new TestName();

    private final String storeName = "hr-store";
    private final long prefix = 1;
    private final long key = 1;
    private final int keySize = 8;
    private final byte[] value = new byte[(int) Chunk.SIZE_LIMIT];

    private File testingHome;
    private MockStoreRegistry store;
    private MetricsRegistry metrics;

    @Before public void setup() throws InterruptedException {
        testingHome = hotRestartHome(getClass(), testName);
        delete(testingHome);
        final HotRestartStoreConfig cfg = new HotRestartStoreConfig();
        cfg.setHomeDir(new File(testingHome, storeName))
           .setLoggingService(createLoggingService())
           .setMetricsRegistry(new MetricsRegistryImpl(cfg.logger(), INFO))
           .setIoDisabled(true);
        metrics = cfg.metricsRegistry();
        store = new MockStoreRegistry(cfg, null);
    }

    @After public void tearDown() {
        delete(testingHome);
    }

    @Test public void liveValuesMetric() {
        withGcPaused(new CatchupRunnable() {
            @Override public void run(CatchupTestSupport mc) {
                final LongGauge liveValues = gauge(".liveValues");
                store.put(prefix, key, value);
                mc.catchupNow();
                assertEquals(1L, liveValues.read());
            }
        });
    }

    @Test public void liveTombstonesMetric() {
        withGcPaused(new CatchupRunnable() {
            @Override public void run(CatchupTestSupport mc) {
                final LongGauge liveTombstones = gauge(".liveTombstones");
                store.put(prefix, key, value);
                store.remove(prefix, key);
                mc.catchupNow();
                assertEquals(1L, liveTombstones.read());
            }
        });
    }

    @Test public void valOccupancyMetric() {
        withGcPaused(new CatchupRunnable() {
            @Override public void run(CatchupTestSupport mc) {
                final LongGauge valOccupancy = gauge(".valOccupancy");
                store.put(prefix, key, value);
                mc.catchupNow();
                assertEquals(Record.VAL_HEADER_SIZE + keySize + value.length, valOccupancy.read());
            }
        });
    }

    @Test public void valGarbageMetric() {
        withGcPaused(new CatchupRunnable() {
            @Override public void run(CatchupTestSupport mc) {
                final LongGauge valGarbage = gauge(".valGarbage");
                store.put(prefix, key, value);
                store.remove(prefix, key);
                mc.catchupNow();
                assertEquals(Record.VAL_HEADER_SIZE + keySize + value.length, valGarbage.read());
            }
        });
    }

    @Test public void tombOccupancyMetric() {
        withGcPaused(new CatchupRunnable() {
            @Override public void run(CatchupTestSupport mc) {
                final LongGauge tombOccupancy = gauge(".tombOccupancy");
                for (int i = 0; i < Chunk.TOMB_COUNT_LIMIT; i++) {
                    store.put(prefix, key, value);
                    store.remove(prefix, key);
                    mc.catchupNow();
                }
                assertEquals(Chunk.TOMB_COUNT_LIMIT * (TOMB_HEADER_SIZE + keySize), tombOccupancy.read());
            }
        });
    }

    @Test public void tombGarbageMetric() {
        withGcPaused(new CatchupRunnable() {
            @Override public void run(CatchupTestSupport mc) {
                final LongGauge tombGarbage = gauge(".tombGarbage");
                for (int i = 0; i < Chunk.TOMB_COUNT_LIMIT + 1; i++) {
                    store.put(prefix, key, value);
                    store.remove(prefix, key);
                    mc.catchupNow();
                }
                assertEquals(Chunk.TOMB_COUNT_LIMIT * (TOMB_HEADER_SIZE + keySize), tombGarbage.read());
            }
        });
    }

    private void withGcPaused(CatchupRunnable r) {
        ((HotRestartStoreImpl) store.hrStore).runWhileGcPaused(r);
    }

    private LongGauge gauge(String name) {
        final LongGauge gauge = metrics.newLongGauge("hot-restart." + storeName + name);
        assertNotNull(gauge);
        assertEquals(0, gauge.read());
        return gauge;
    }
}
