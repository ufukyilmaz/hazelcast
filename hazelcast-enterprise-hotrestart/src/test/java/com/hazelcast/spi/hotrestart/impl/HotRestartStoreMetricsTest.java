package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.spi.hotrestart.impl.testsupport.MockStoreRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.concurrent.Callable;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
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
    private final byte[] value = new byte[1];

    private MockStoreRegistry store;
    private MetricsRegistry metrics;

    @Before public void setup() {
        final File testingHome = hotRestartHome(getClass(), testName);
        final HotRestartStoreConfig cfg = new HotRestartStoreConfig();
        cfg.setHomeDir(new File(testingHome, storeName))
           .setLoggingService(createLoggingService())
           .setMetricsRegistry(new MetricsRegistryImpl(cfg.logger(), INFO));
        metrics = cfg.metricsRegistry();
        store = new MockStoreRegistry(cfg, null);
    }

    @Test public void liveValuesMetric() {
        final LongGauge liveValues = gauge(".liveValues");
        store.put(prefix, key, value);
        assertEqualsEventually(readGauge(liveValues), 1L);
    }

    @Test public void liveTombstonesMetric() {
        final LongGauge liveTombstones = gauge(".liveTombstones");
        store.put(prefix, key, value);
        store.remove(prefix, key);
        assertEqualsEventually(readGauge(liveTombstones), 1L);
    }

    private LongGauge gauge(String name) {
        final LongGauge gauge = metrics.newLongGauge("hot-restart." + storeName + name);
        assertNotNull(gauge);
        assertEquals(0, gauge.read());
        return gauge;
    }

    private static Callable<Long> readGauge(final LongGauge gauge) {
        return new Callable<Long>() {
            @Override public Long call() {
                return gauge.read();
            }
        };
    }
}
