package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup.CatchupRunnable;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.testsupport.MockStoreRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.SYSPROP_TOMB_CHUNK_SIZE_LIMIT;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.SYSPROP_VAL_CHUNK_SIZE_LIMIT;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.TOMB_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.runWithPausedGC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartStoreMetricsTest extends HazelcastTestSupport {

    @Rule
    public final TestName testName = new TestName();

    private final String storeName = "hr-store";
    private final long prefix = 1;
    private final long key = 1;
    private final int keySize = 8;

    private byte[] value;
    private File testingHome;
    private MetricsRegistry metrics;
    private MockStoreRegistry store;

    @Before
    public void setup() {
        System.setProperty(SYSPROP_TOMB_CHUNK_SIZE_LIMIT, String.valueOf(8));
        System.setProperty(SYSPROP_VAL_CHUNK_SIZE_LIMIT, String.valueOf(8));
        value = new byte[valChunkSizeLimit()];
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        final HotRestartStoreConfig cfg = new HotRestartStoreConfig();
        cfg.setStoreName(storeName).setHomeDir(new File(testingHome, storeName))
                .setLoggingService(createLoggingService())
                .setMetricsRegistry(new MetricsRegistryImpl(cfg.logger(), MANDATORY));
        metrics = cfg.metricsRegistry();
        store = new MockStoreRegistry(cfg, null, true);
    }

    @After
    public void tearDown() {
        store.closeHotRestartStore();
        delete(testingHome);
    }

    @Test
    public void liveValuesMetric() {
        runWithPausedGC(store, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                final LongGauge liveValues = gauge(".liveValues");
                store.put(prefix, key, value);
                mc.catchupNow();
                assertEquals(1L, liveValues.read());
            }
        });
    }

    @Test
    public void liveTombstonesMetric() {
        runWithPausedGC(store, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                final LongGauge liveTombstones = gauge(".liveTombstones");
                store.put(prefix, key, value);
                store.remove(prefix, key);
                mc.catchupNow();
                assertEquals(1L, liveTombstones.read());
            }
        });
    }

    @Test
    public void valOccupancyMetric() {
        runWithPausedGC(store, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                final LongGauge valOccupancy = gauge(".valOccupancy");
                store.put(prefix, key, value);
                mc.catchupNow();
                assertEquals(Record.VAL_HEADER_SIZE + keySize + value.length, valOccupancy.read());
            }
        });
    }

    @Test
    public void valGarbageMetric() {
        runWithPausedGC(store, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                final LongGauge valGarbage = gauge(".valGarbage");
                store.put(prefix, key, value);
                store.remove(prefix, key);
                mc.catchupNow();
                assertEquals(Record.VAL_HEADER_SIZE + keySize + value.length, valGarbage.read());
            }
        });
    }

    @Test
    public void tombOccupancyMetric() {
        runWithPausedGC(store, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                final LongGauge tombOccupancy = gauge(".tombOccupancy");
                store.put(prefix, key, value);
                store.remove(prefix, key);
                mc.catchupNow();
                assertEquals(TOMB_HEADER_SIZE + keySize, tombOccupancy.read());
            }
        });
    }

    @Test
    public void tombGarbageMetric() {
        runWithPausedGC(store, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                final LongGauge tombGarbage = gauge(".tombGarbage");
                store.put(prefix, key, value);
                store.remove(prefix, key);
                store.put(prefix, key, value);
                store.remove(prefix, key);
                mc.catchupNow();
                assertEquals(TOMB_HEADER_SIZE + keySize, tombGarbage.read());
            }
        });
    }

    private LongGauge gauge(String name) {
        final LongGauge gauge = metrics.newLongGauge("hot-restart." + storeName + name);
        assertNotNull(gauge);
        assertEquals(0, gauge.read());
        return gauge;
    }
}
