package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.GcRecord.WithHeapHandle;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hotRestartHome;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.metricsRegistry;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EvacuatorTest {

    @Rule public final TestName testName = new TestName();

    private final byte[] mockValue = new byte[1];
    private Evacuator ev;

    @Before public void setup() {
        final MutatorCatchup mc = mock(MutatorCatchup.class);
        final LoggingService loggingService = createLoggingService();
        final HotRestartStoreConfig hrConfig = new HotRestartStoreConfig()
                .setHomeDir(hotRestartHome(getClass(), testName))
                .setLoggingService(loggingService)
                .setMetricsRegistry(metricsRegistry(loggingService));
        final GcHelper.OnHeap gcHelper = new GcHelper.OnHeap(hrConfig);
        final ChunkManager chunkMgr = new ChunkManager(hrConfig, gcHelper, null);
        ev = new Evacuator(null, chunkMgr, mc, null, 0);
    }

    @Test public void testSorting() {
        final int size = 5;
        final ArrayList<GcRecord> gcrs = new ArrayList<GcRecord>(size);
        for (int seq = size; seq >= 1; seq--) {
            gcrs.add(gcRecord(seq));
        }
        final List<GcRecord> sorted = ev.sorted(gcrs);
        assertEquals(size, sorted.size());
        int seq = 1;
        for (Record r : sorted) {
            assertEquals(seq++, r.liveSeq());
        }
    }

    private WithHeapHandle gcRecord(long seq) {
        return new WithHeapHandle(new RecordOnHeap(seq, 0, false, 0), 1, new KeyOnHeap(1, mockValue));
    }
}
