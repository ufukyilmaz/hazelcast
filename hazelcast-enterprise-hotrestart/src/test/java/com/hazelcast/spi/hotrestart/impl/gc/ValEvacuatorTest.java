package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createLoggingService;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.hotRestartHome;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.metricsRegistry;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ValEvacuatorTest {

    @Rule public final TestName testName = new TestName();

    private final byte[] mockValue = new byte[1];
    private ValEvacuator ev;

    @Before public void setup() {
        final MutatorCatchup mc = mock(MutatorCatchup.class);
        final LoggingService loggingService = createLoggingService();
        final HotRestartStoreConfig hrConfig = new HotRestartStoreConfig()
                .setHomeDir(hotRestartHome(getClass(), testName))
                .setLoggingService(loggingService)
                .setMetricsRegistry(metricsRegistry(loggingService));
        final GcHelper.OnHeap gcHelper = new GcHelper.OnHeap(hrConfig);
        final ChunkManager chunkMgr = new ChunkManager(hrConfig, gcHelper, null);
        final Collection<StableValChunk> srcChunks = new ArrayList<StableValChunk>();
        ev = new ValEvacuator(srcChunks, chunkMgr, mc, null, 0);
    }
}
